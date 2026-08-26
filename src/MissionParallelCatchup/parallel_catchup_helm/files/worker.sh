#!/bin/sh

# Check if required environment variables are set
if [ -z "$REDIS_HOST" ]; then echo "REDIS_HOST not set"; exit 1; fi
if [ -z "$REDIS_PORT" ]; then echo "REDIS_PORT not set"; exit 1; fi
if [ -z "$JOB_QUEUE" ]; then echo "JOB_QUEUE not set"; exit 1; fi
if [ -z "$PROGRESS_QUEUE" ]; then echo "PROGRESS_QUEUE not set"; exit 1; fi
if [ -z "$FAILED_QUEUE" ]; then echo "FAILED_QUEUE not set"; exit 1; fi
if [ -z "$SUCCESS_QUEUE" ]; then echo "SUCCESS_QUEUE not set"; exit 1; fi
if [ -z "$METRICS" ]; then echo "METRICS not set"; exit 1; fi
if [ -z "$JOB_OWNERS" ]; then echo "JOB_OWNERS not set"; exit 1; fi
if [ -z "$RELEASE_NAME" ]; then echo "RELEASE_NAME not set"; exit 1; fi
if [ -z "$POD_NAME" ]; then echo "POD_NAME not set"; exit 1; fi

# ensure redis-cli is available
if [ ! "$(redis-cli --version)" ]; then
    echo "redis-cli not found, please ensure running with a supported stellar-core version"
    exit 1
fi

SLEEP_INTERVAL=10
LOG_DIR="/data"
# The driver adds a pod here when it intends to remove that worker.
RETIRING_SET="$RELEASE_NAME-retiring"
# ...and we add ourselves here to say we have seen it and are between jobs.
RETIRED_SET="$RELEASE_NAME-retired"
# Heartbeat: proves to the driver that this pod is running worker.sh right now.
READY_SET="$RELEASE_NAME-ready"

while true; do
# Beat first. The driver counts recent beats as its available capacity and picks
# a beating pod to run redis commands in. A Pending pod cannot beat, so it is
# never counted as spare capacity and never chosen as the command host.
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ZADD "$READY_SET" "$(date +%s)" "$POD_NAME" >/dev/null

# Stop claiming once the driver has marked us retiring, so it can delete us
# without interrupting a range. Only an exact "1" bars a claim: any other reply,
# including an error or empty output, claims as before rather than idling the
# fleet over a transient Redis blip.
RETIRING=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SISMEMBER "$RETIRING_SET" "$POD_NAME")
if [ "$RETIRING" = "1" ]; then
    # Announce it. This runs at the top of the loop, after any previous job's
    # completion transaction and before the next claim, so a pod in the retired
    # set provably holds no job -- and having seen the mark, it never claims
    # again. The driver deletes only announced pods, so it never has to judge
    # idleness from a status snapshot whose age it cannot bound.
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" SADD "$RETIRED_SET" "$POD_NAME" >/dev/null
    echo "$(date) $POD_NAME is marked retiring; not claiming."
    sleep $SLEEP_INTERVAL
    continue
fi

# Fetch the next job key from the Redis queue.
# Our ranges are generated in the order we want to run them from left to right, so we always pull from the left
JOB_KEY=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" LMOVE "$JOB_QUEUE" "$PROGRESS_QUEUE" LEFT LEFT)
LMOVE_EXIT_CODE=$?

# Only process a job if the command succeeded AND we got a non-empty job key
if [ $LMOVE_EXIT_CODE -eq 0 ] && [ -n "$JOB_KEY" ]; then
    # Register ownership so the monitor knows which worker owns this job
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HSET "$JOB_OWNERS" "$JOB_KEY" "$POD_NAME"
    if [ $? -ne 0 ]; then
        echo "Error: Failed to register job ownership for $JOB_KEY. Exiting."
        exit 1
    fi

    # Start timer
    START_TIME=$(date +%s)
    echo "Processing job: $JOB_KEY"

    # Run stellar-core: create new-db then catchup
    /usr/bin/stellar-core --conf /config/stellar-core.cfg new-db --console && \
    /usr/bin/stellar-core --conf /config/stellar-core.cfg catchup "$JOB_KEY" \
        --metric 'ledger.transaction.apply' --console
    STELLAR_CORE_EXIT_CODE=$?

    # End timer and duration
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))s
    echo "Finish processing job: $JOB_KEY, duration: $DURATION"

    # Check if both commands succeeded
    if [ $STELLAR_CORE_EXIT_CODE -eq 0 ]; then
        echo "Successfully processed job: $JOB_KEY"
        QUEUE_COMMAND="LPUSH $SUCCESS_QUEUE \"$JOB_KEY\""
    else
        echo "Error processing job: $JOB_KEY (exit code: $STELLAR_CORE_EXIT_CODE)"
        QUEUE_COMMAND="LPUSH $FAILED_QUEUE \"$JOB_KEY|$POD_NAME\""
    fi

    # Parse and extract the metrics from the log file
    LOG_FILE=$(ls -t "$LOG_DIR"/stellar-core*.log 2>/dev/null | head -n 1)
    if [ -z "$LOG_FILE" ]; then
        echo "No log file found in $LOG_DIR"
        exit 1
    fi

    tx_apply_ms=$(tac "$LOG_FILE" | grep -m 1 -B 11 "metric 'ledger.transaction.apply':" | grep "sum =" | awk '{print $NF}')
    echo "Log file: $LOG_FILE"
    echo "ledger.transaction.apply sum: $tx_apply_ms"
    # Validate metric was extracted successfully
    if [ -z "$tx_apply_ms" ]; then
        echo "Warning: Failed to extract metric 'ledger.transaction.apply' from log file"
        tx_apply_ms="N/A"
    fi

    # Push metrics to redis in a transaction to ensure data consistency. Retry for 5min on failures
    # The whole pod name. With one replica per StatefulSet the trailing segment
    # is always "0", so it no longer identifies the worker.
    core_id=$POD_NAME

    result=1  # Initialize to failure
    for i in $(seq 1 30);do
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" <<EOF
MULTI
$QUEUE_COMMAND
LREM "$PROGRESS_QUEUE" -1 "$JOB_KEY"
SADD "$METRICS" "$JOB_KEY|$core_id|$tx_apply_ms|$DURATION"
HDEL "$JOB_OWNERS" "$JOB_KEY"
EXEC
EOF
        result=$?
        if [ $result -ne 0 ]; then
            echo "Redis transaction failed. Sleeping and retrying (attempt $i/30)"
            sleep 10
        else
            break
        fi
    done    
    # Check if all retries were exhausted
    if [ "$result" -ne 0 ]; then
        echo "Error: Redis transaction failed after all 30 retry attempts. Exiting."
        exit 1
    fi

else
    # Either Redis command failed OR queue is empty
    if [ $LMOVE_EXIT_CODE -ne 0 ]; then
        echo "Error: Failed to connect to Redis at $REDIS_HOST:$REDIS_PORT"
        echo "Exit code=$LMOVE_EXIT_CODE, Output: $JOB_KEY"
    else
        echo "$(date) No more jobs in the queue."
    fi
    echo "Sleeping for $SLEEP_INTERVAL seconds..."
    sleep $SLEEP_INTERVAL
fi
done
