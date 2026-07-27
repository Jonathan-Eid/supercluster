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
# All of these live on /data so they travel with the DB and cannot disagree with it.
JOB_MARKER="/data/current_job"   # job whose partial state is on this volume
CLEAN_MARKER="/data/clean"       # written only by the SIGTERM trap below
NODE_MARKER="/data/last_node"    # node the previous incarnation ran on
START_MARKER="/data/job_started" # "<job key> <epoch>" of the FIRST claim of that job

# Forward SIGTERM to stellar-core instead of dying without it. Without this the
# shell sits in `wait` for the whole grace period and stellar-core is SIGKILLed
# (container exit 137) having never run gracefulStop(), so the DB is never
# flushed. Requires stellar-core to run in the background -- dash defers traps
# until a *foreground* child exits.
CORE_PID=""
on_term() {
    if [ -n "$CORE_PID" ]; then
        echo "SIGTERM: forwarding to stellar-core (pid $CORE_PID) for graceful shutdown"
        kill -TERM "$CORE_PID" 2>/dev/null
        wait "$CORE_PID" 2>/dev/null
        : > "$CLEAN_MARKER"
    fi
    # Eviction, not a catchup failure: leave the job in PROGRESS_QUEUE under our
    # ownership so the rescheduled pod resumes it. Touch neither queue.
    exit 143
}
trap on_term TERM INT

# Classify how the previous incarnation died. SIGTERM is trappable and SIGKILL is
# not, so a missing CLEAN_MARKER means an uncatchable kill; the node name then
# separates "container restarted in place" (OOM-kill/crash -- must fail loudly so
# requests/limits get retuned) from "pod was rescheduled" (eviction -- resume).
if [ -f "$JOB_MARKER" ] && [ ! -f "$CLEAN_MARKER" ] && [ -f "$NODE_MARKER" ]; then
    PREV_NODE=$(cat "$NODE_MARKER")
    OOM_JOB=$(cat "$JOB_MARKER")
    if [ -n "$OOM_JOB" ] && [ "$PREV_NODE" = "${NODE_NAME:-}" ]; then
        echo "Previous run of $OOM_JOB died uncleanly on the same node ($PREV_NODE): OOM-kill or crash, not an eviction. Failing the job."
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" <<EOF
MULTI
LPUSH "$FAILED_QUEUE" "$OOM_JOB|$POD_NAME"
LREM "$PROGRESS_QUEUE" 0 "$OOM_JOB"
HDEL "$JOB_OWNERS" "$OOM_JOB"
EXEC
EOF
        rm -f "$JOB_MARKER" "$NODE_MARKER" "$CLEAN_MARKER" "$START_MARKER"
    fi
fi

# When /data is a PVC that outlived a node loss, reclaim the job this pod was
# already running rather than taking a new one: resuming is only valid if the
# range matches the partial DB on this volume.
RESUME_JOB=""
if [ "${RESUME_SKIP_NEWDB:-false}" = "true" ] && [ -f "$JOB_MARKER" ] && [ -f /data/stellar.db ]; then
    CANDIDATE=$(cat "$JOB_MARKER")
    if [ -n "$CANDIDATE" ]; then
        OWNER=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" HGET "$JOB_OWNERS" "$CANDIDATE")
        if [ "$OWNER" = "$POD_NAME" ]; then
            echo "RESUME: reclaiming own in-progress job $CANDIDATE from /data"
            RESUME_JOB="$CANDIDATE"
        elif [ -z "$OWNER" ]; then
            # The monitor re-queued this job and cleared its owner (it does that
            # once every active worker looks down). Take it back only if we can
            # pull it off the queue first, so no other worker also runs it.
            # LREM+LPUSH must be ONE atomic step: if the monitor samples between
            # them the job is in neither queue and the driver concludes "all
            # queues empty" and declares the mission complete without the range
            # ever being caught up.
            REMOVED=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EVAL \
              "local moved = redis.call('LREM', KEYS[1], 0, ARGV[1]) if moved >= 1 then redis.call('LPUSH', KEYS[2], ARGV[1]) end return moved" \
              2 "$JOB_QUEUE" "$PROGRESS_QUEUE" "$CANDIDATE")
            if [ "${REMOVED:-0}" -ge 1 ] 2>/dev/null; then
                echo "RESUME: re-claimed re-queued job $CANDIDATE from /data"
                RESUME_JOB="$CANDIDATE"
            else
                echo "RESUME: job $CANDIDATE already taken by another worker; discarding partial state"
                rm -f "$JOB_MARKER" "$START_MARKER"
            fi
        else
            echo "RESUME: marker job $CANDIDATE owned by $OWNER; discarding partial state"
            rm -f "$JOB_MARKER" "$START_MARKER"
        fi
    fi
fi

while true; do
if [ -n "$RESUME_JOB" ]; then
    # Already in PROGRESS_QUEUE under our ownership. Drop any copy the monitor
    # re-queued so the same range cannot also be handed to another worker.
    JOB_KEY="$RESUME_JOB"
    RESUME_JOB=""
    # The monitor may have re-queued this job while we were starting up. Drop any
    # copy from JOB_QUEUE, but ONLY as part of putting it back in PROGRESS_QUEUE:
    # a bare LREM here can delete the last copy, leaving the job in no queue at
    # all, which the driver reads as "all queues empty" -> false mission success.
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EVAL \
      "local moved = redis.call('LREM', KEYS[1], 0, ARGV[1]) local inprog = 0 for _,v in ipairs(redis.call('LRANGE', KEYS[2], 0, -1)) do if v == ARGV[1] then inprog = 1 end end if inprog == 0 then redis.call('LPUSH', KEYS[2], ARGV[1]) end return moved" \
      2 "$JOB_QUEUE" "$PROGRESS_QUEUE" "$JOB_KEY" >/dev/null
    LMOVE_EXIT_CODE=0
else
# Fetch the next job key from the Redis queue.
# Our ranges are generated in the order we want to run them from left to right, so we always pull from the left
# Claim and ownership-record must be ONE atomic step. Split across two commands
# there is a window where the job is in `in_progress` with no entry in
# `job_owners`, so the worker looks idle to anything reading ownership -- and a
# reaper would neuter a pod that is mid-job.
JOB_KEY=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" EVAL \
  "local job = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'LEFT') if job then redis.call('HSET', KEYS[3], job, ARGV[1]) end return job" \
  3 "$JOB_QUEUE" "$PROGRESS_QUEUE" "$JOB_OWNERS" "$POD_NAME")
LMOVE_EXIT_CODE=$?
fi

# Only process a job if the command succeeded AND we got a non-empty job key
if [ $LMOVE_EXIT_CODE -eq 0 ] && [ -n "$JOB_KEY" ]; then

    # Start timer. A pod reclaimed mid-job returns under the same StatefulSet
    # name and re-enters this loop for the SAME job, so timing from here would
    # only measure the resumed segment and report a nearly-finished job as fast
    # -- biasing the duration histogram in spot's favour. Carry the first-claim
    # timestamp on /data instead, keyed by job so a stale one cannot be reused.
    START_TIME=""
    if [ -f "$START_MARKER" ]; then
        PRIOR_JOB=$(cut -d' ' -f1 "$START_MARKER" 2>/dev/null)
        PRIOR_TS=$(cut -d' ' -f2 "$START_MARKER" 2>/dev/null)
        if [ "$PRIOR_JOB" = "$JOB_KEY" ] && [ -n "$PRIOR_TS" ]; then
            START_TIME=$PRIOR_TS
            echo "RESUME: job $JOB_KEY first claimed at epoch $START_TIME, timing from there"
        fi
    fi
    if [ -z "$START_TIME" ]; then
        START_TIME=$(date +%s)
        printf '%s %s' "$JOB_KEY" "$START_TIME" > "$START_MARKER"
    fi
    echo "Processing job: $JOB_KEY"

    # Run stellar-core: (conditionally) new-db, then catchup.
    # Skip new-db only when persistent /data holds partial state for THIS range
    # (an EBS PVC that survived a pod restart); catchup then resumes from the
    # last-closed ledger. A DB from any other range must be discarded.
    RESUMING=false
    if [ "${RESUME_SKIP_NEWDB:-false}" = "true" ] && [ -f /data/stellar.db ] \
       && [ -f "$JOB_MARKER" ] && [ "$(cat "$JOB_MARKER")" = "$JOB_KEY" ]; then
        # Resuming is only safe once ledger REPLAY has begun. Bucket apply uses
        # createWithoutLoading() -- an unconditional INSERT that assumes a fresh
        # DB -- so re-applying buckets over a partially applied DB hits primary
        # key conflicts and fails the job.
        #
        # Read the last close out of the previous run's core log, which lives on
        # /data and so survives the reclaim. Do NOT ask the DB: stellar-core 27
        # (schema v28) dropped the `ledgerheaders` table and keeps the LCL as a
        # base64 XDR blob in `storestate`, so the old
        #   SELECT MAX(ledgerseq) FROM ledgerheaders
        # silently errored to empty on every reclaim and no job ever resumed.
        # A logged close is also a more direct signal than a DB row: it means
        # replay actually produced a ledger, which is exactly the precondition.
        TARGET=${JOB_KEY%%/*}
        COUNT=${JOB_KEY##*/}
        # Newest log here is the previous incarnation's -- core for this run has
        # not started yet.
        PREV_LOG=$(ls -t "$LOG_DIR"/stellar-core*.log 2>/dev/null | head -n 1)
        LCL=""
        if [ -n "$PREV_LOG" ]; then
            LCL=$(grep -oE "Ledger close complete: [0-9]+" "$PREV_LOG" 2>/dev/null | tail -1 | grep -oE '[0-9]+$')
        fi
        # Bound on both sides: a leftover log from a different range on this same
        # worker must not be mistaken for progress on this job.
        if [ -n "$LCL" ] && [ "$LCL" -ge $((TARGET - COUNT)) ] && [ "$LCL" -le "$TARGET" ] 2>/dev/null; then
            RESUMING=true
            echo "RESUME: previous run of $JOB_KEY reached ledger $LCL; replay had begun, resuming"
        else
            echo "RESUME: no in-range ledger close found for $JOB_KEY (last=${LCL:-none}); bucket phase incomplete, starting fresh"
        fi
    fi
    # Claim /data for this job before touching the DB, so an eviction between
    # here and the first ledger close still leaves a correct marker. last_node
    # records where we ran; clean is stale from here on and must not linger.
    printf '%s' "$JOB_KEY" > "$JOB_MARKER"
    printf '%s' "${NODE_NAME:-unknown}" > "$NODE_MARKER"
    rm -f "$CLEAN_MARKER"

    NEWDB_RC=0
    if [ "$RESUMING" = "true" ]; then
        echo "RESUME: partial DB on /data for $JOB_KEY, skipping new-db"
    else
        /usr/bin/stellar-core --conf /config/stellar-core.cfg new-db --console
        NEWDB_RC=$?
    fi
    if [ "$NEWDB_RC" -eq 0 ]; then
        # Backgrounded + wait so the SIGTERM trap can run promptly; dash defers
        # traps until a foreground child exits.
        /usr/bin/stellar-core --conf /config/stellar-core.cfg catchup "$JOB_KEY" \
            --metric 'ledger.transaction.apply' --console &
        CORE_PID=$!
        wait "$CORE_PID"
        STELLAR_CORE_EXIT_CODE=$?
        CORE_PID=""
    else
        STELLAR_CORE_EXIT_CODE=$NEWDB_RC
    fi

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

    # Drop the markers BEFORE reporting the result. stellar-core has exited, so
    # they have no further use, and an uncatchable kill between the report and a
    # later cleanup would otherwise look like "died mid-job on the same node" and
    # fail a job that already succeeded. Being killed here instead just leaves the
    # job in PROGRESS_QUEUE for the monitor to re-queue, which is recoverable.
    rm -f "$JOB_MARKER" "$CLEAN_MARKER" "$NODE_MARKER" "$START_MARKER"

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
    # Extract the pod ordinal (last hyphen-separated segment) from pod name like "release-name-stellar-core-0"
    core_id=$(echo "$POD_NAME" | awk -F'-' '{print $NF}')
    # Validate core_id was extracted successfully
    if [ -z "$core_id" ]; then
        echo "Error: Failed to extract core_id from POD_NAME: $POD_NAME"
        core_id="N/A"
    fi

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

    # >>> TEST-ONLY INJECTION (revert before commit) <<<
    # Recreate the markers AFTER the result was reported -- i.e. reproduce the old
    # ordering -- then die uncleanly so the container restarts in place on the same
    # node. That is the exact state the reordering was meant to make unreachable.
    # Markers were already dropped before the result was reported (see above), so
    # the next job on this worker starts from a fresh DB via new-db.

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
