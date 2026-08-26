// Copyright 2024 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

module MissionHistoryPubnetParallelCatchupV2

open Logging
open ScriptUtils
open StellarKubeSpecs
open StellarMissionContext
open StellarNetworkData
open StellarNetworkCfg
open StellarSupercluster

open System
open System.Diagnostics
open System.Net
open System.Net.Http
open System.IO

open Newtonsoft.Json.Linq
open Microsoft.FSharp.Control
open System.Threading
open System

open k8s
open CSLibrary

// Constants
let helmChartPath = "/supercluster/src/MissionParallelCatchup/parallel_catchup_helm"

// Comment out the path below for local testing
// Example command to run local testing (in the `supercluster/` directory):
// $ dotnet run --project src/App/App.fsproj -- mission HistoryPubnetParallelCatchupV2 --image=docker-registry.services.stellar-ops.com/dev/stellar-core:23.0.3-2779.4d1df2b03.jammy-vnext-buildtests  --pubnet-parallel-catchup-num-workers=2 --pubnet-parallel-catchup-starting-ledger=0 --pubnet-parallel-catchup-end-ledger=6400 --pubnet-parallel-catchup-ledgers-per-job 1280  --destination ./logs
// let helmChartPath = "src/MissionParallelCatchup/parallel_catchup_helm"
let valuesFilePath = helmChartPath + "/values.yaml"

let defaultJobMonitorHostName = "ssc-job-monitor-eks.services.stellar-ops.com"
let jobMonitorStatusEndPoint = "/status"
let jobMonitorMetricsEndPoint = "/metrics"
let jobMonitorLoggingIntervalSecs = 30 // frequency of job monitor's internal information gathering (querying core endpoint and redis metrics) and logging
let jobMonitorStatusCheckIntervalSecs = 60 // frequency of us querying job monitor's `/status` end point
let jobMonitorMetricsCheckIntervalSecs = 60 // frequency of us querying job monitor's `/metrics` end point
let jobMonitorStatusCheckTimeOutSecs = 600
let mutable toPerformCleanup = true
let failedJobLogFileLineCount = 10000
let failedJobLogStreamLineCount = 1000

let mutable nonce : String = ""
// Workers not yet retired. Retired workers' logs are collected as they go, so
// cleanup must not try to collect from a pod that is already deleted.
let mutable liveWorkers : Set<int> = Set.empty

let mutable helmReleaseName : String = ""

let jobMonitorHostName (context: MissionContext) =
    match context.jobMonitorExternalHost with
    | Some host -> host
    | None -> defaultJobMonitorHostName // TODO: append it with a nounce to make it session specific

// Helper functions to convert label/taint tuples to Helm-compatible format using indexed notation
let requireNodeLabelToHelmIndexed (index: int) ((key: string), (value: string option)) =
    match value with
    | None -> sprintf "worker.requireNodeLabels[%d].key=%s,worker.requireNodeLabels[%d].operator=Exists" index key index
    | Some v ->
        sprintf
            "worker.requireNodeLabels[%d].key=%s,worker.requireNodeLabels[%d].operator=In,worker.requireNodeLabels[%d].values[0]=\"%s\""
            index
            key
            index
            index
            v

let avoidNodeLabelToHelmIndexed (index: int) ((key: string), (value: string option)) =
    match value with
    | None ->
        sprintf "worker.avoidNodeLabels[%d].key=%s,worker.avoidNodeLabels[%d].operator=DoesNotExist" index key index
    | Some v ->
        sprintf
            "worker.avoidNodeLabels[%d].key=%s,worker.avoidNodeLabels[%d].operator=NotIn,worker.avoidNodeLabels[%d].values[0]=\"%s\""
            index
            key
            index
            index
            v

let tolerateTaintToHelmIndexed (index: int) ((key: string), (effect: string option)) =
    let effectValue = Option.defaultValue "NoSchedule" effect
    sprintf "worker.tolerateNodeTaints[%d].key=%s,worker.tolerateNodeTaints[%d].effect=%s" index key index effectValue

let serviceAccountAnnotationsToHelmIndexed (index: int) (key: string, value: string) =
    sprintf "service_account.annotations[%d].key=%s,service_account.annotations[%d].value=%s" index key index value

let installProject (context: MissionContext) =
    LogInfo "Installing Helm chart with release name: %s" helmReleaseName

    // install the project with default values from the file and overridden values from the commandline
    let setOptions = ResizeArray<string>()
    setOptions.Add(sprintf "worker.stellar_core_image=%s" context.image)
    setOptions.Add(sprintf "worker.replicas=%d" context.pubnetParallelCatchupNumWorkers)

    // Set Redis hostname to be unique per release
    setOptions.Add(sprintf "redis.hostname=%s-redis" nonce)

    setOptions.Add(sprintf "range_generator.params.starting_ledger=%d" context.pubnetParallelCatchupStartingLedger)

    let endLedger =
        match context.pubnetParallelCatchupEndLedger with
        | Some value -> value
        | None -> GetLatestPubnetLedgerNumber()

    setOptions.Add(sprintf "range_generator.params.latest_ledger_num=%d" endLedger)

    setOptions.Add(
        sprintf "range_generator.params.uniform_ledgers_per_job=%d" context.pubnetParallelCatchupLedgersPerJob
    )

    // Skip known results by default
    setOptions.Add(
        sprintf
            "worker.catchup_skip_known_results_for_testing=%b"
            (Option.defaultValue true context.catchupSkipKnownResultsForTesting)
    )
    // Check events consistency invariant by default
    setOptions.Add(
        sprintf
            "worker.check_events_are_consistent_with_entry_diffs=%b"
            (Option.defaultValue true context.checkEventsAreConsistentWithEntryDiffs)
    )

    // read the resource requirements defined in StellarKubeSpecs.fs (where resource for various missions are centralized)
    let resourceRequirements = ParallelCatchupCoreResourceRequirements
    let cpuReqMili = resourceRequirements.Requests.["cpu"].ToString()
    let memReqMebi = resourceRequirements.Requests.["memory"].ToString()
    let cpuLimMili = resourceRequirements.Limits.["cpu"].ToString()
    let memLimMebi = resourceRequirements.Limits.["memory"].ToString()
    let storageReqGibi = resourceRequirements.Requests.["ephemeral-storage"].ToString()
    let storageLimGibi = resourceRequirements.Limits.["ephemeral-storage"].ToString()

    LogInfo
        "Resource requirements from StellarKubeCfg:\n\
             CPU request: %s\n\
             CPU limit: %s\n\
             Memory request: %s\n\
             Memory limit: %s\n\
             Storage request: %s\n\
             Storage limit: %s"
        cpuReqMili
        cpuLimMili
        memReqMebi
        memLimMebi
        storageReqGibi
        storageLimGibi

    setOptions.Add(sprintf "worker.resources.requests.cpu=%s" cpuReqMili)
    setOptions.Add(sprintf "worker.resources.requests.memory=%s" memReqMebi)
    setOptions.Add(sprintf "worker.resources.limits.cpu=%s" cpuLimMili)
    setOptions.Add(sprintf "worker.resources.limits.memory=%s" memLimMebi)
    setOptions.Add(sprintf "worker.resources.requests.ephemeral_storage=%s" storageReqGibi)
    setOptions.Add(sprintf "worker.resources.limits.ephemeral_storage=%s" storageLimGibi)

    // Construct command for fetching history files from S3 for core node
    // `index` and set the corresponding Helm option
    let setS3HistoryGetCommand (url: string) (index: int) =
        if index < 1 || index > 3 then
            failwith "s3HistoryGetCommand: index must be between 1 and 3 inclusive"

        let s3GetCommandBase = sprintf "aws s3 cp --region %s" context.s3HistoryMirrorRegionPcV2
        let command = sprintf "%s s3://%s/core_live_00%d/{0} {1}" s3GetCommandBase url index
        setOptions.Add(sprintf "worker.historyGetCommandCore00%d=\"%s\"" index command)


    match context.s3HistoryMirrorOverridePcV2 with
    | Some mirrorUrl -> [ 1 .. 3 ] |> List.iter (setS3HistoryGetCommand mirrorUrl)
    | None -> ()

    setOptions.Add(sprintf "monitor.hostname=%s" (jobMonitorHostName context))
    setOptions.Add(sprintf "monitor.path_prefix=/%s/%s" context.namespaceProperty helmReleaseName)
    setOptions.Add(sprintf "monitor.logging_interval_seconds=%d" jobMonitorLoggingIntervalSecs)
    // Attach the job-monitor HTTPRoute to the same Gateway as the core route
    // (--gateway-name/--gateway-namespace), instead of the values.yaml defaults.
    setOptions.Add(sprintf "monitor.gateway_name=%s" context.gatewayName)
    setOptions.Add(sprintf "monitor.gateway_namespace=%s" context.gatewayNamespace)

    // Set ASAN_OPTIONS if provided
    match context.asanOptions with
    | Some asanOpts -> setOptions.Add(sprintf "worker.asanOptions=%s" asanOpts)
    | None -> ()

    // Convert labels and taints to Helm array format
    if not (List.isEmpty context.requireNodeLabelsPcV2) then
        let requireLabelsHelm =
            context.requireNodeLabelsPcV2
            |> List.mapi requireNodeLabelToHelmIndexed
            |> String.concat ","

        setOptions.Add(requireLabelsHelm)

    if not (List.isEmpty context.avoidNodeLabelsPcV2) then
        let avoidLabelsHelm =
            context.avoidNodeLabelsPcV2
            |> List.mapi avoidNodeLabelToHelmIndexed
            |> String.concat ","

        setOptions.Add(avoidLabelsHelm)

    if not (List.isEmpty context.tolerateNodeTaintsPcV2) then
        let tolerateTaintsHelm =
            context.tolerateNodeTaintsPcV2
            |> List.mapi tolerateTaintToHelmIndexed
            |> String.concat ","

        setOptions.Add(tolerateTaintsHelm)

    match context.serviceAccountAnnotationsPcV2 with
    | [] -> ()
    | _ ->
        context.serviceAccountAnnotationsPcV2
        |> List.mapi serviceAccountAnnotationsToHelmIndexed
        |> String.concat ","
        |> setOptions.Add

    // Expand tilde in kubeconfig path before setting environment variable
    let expandedKubeCfg = ExpandHomeDirTilde context.kubeCfg
    Environment.SetEnvironmentVariable("KUBECONFIG", expandedKubeCfg)

    RunShellCommand [| "helm"
                       "install"
                       helmReleaseName
                       helmChartPath
                       "--values"
                       valuesFilePath
                       "--set"
                       String.Join(",", setOptions) |]
    |> ignore

    match RunShellCommand [| "helm"
                             "get"
                             "values"
                             helmReleaseName |] with
    | Some valuesOutput -> LogInfo "%s" valuesOutput
    | _ -> ()

// Removal is synchronous inside the status loop: every departing worker is
// exec'd into and its archive streamed back before the delete. Cap the batch so
// one pass cannot stall polling for minutes; later passes keep shrinking.
let maxWorkersRetiredPerPass = 32

// Each worker is its own single-replica StatefulSet, so both names follow from
// the index alone and the driver never has to list pods.
let workerStatefulSetName (index: int) = sprintf "%s-stellar-core-%d" helmReleaseName index

let workerPodName (index: int) = sprintf "%s-0" (workerStatefulSetName index)

// A worker whose heartbeat is older than this is not counted as capacity and is
// not used to run commands. worker.sh beats every SLEEP_INTERVAL (10s).
let readyStaleSecs = 45

let indexOfPodName (podName: string) : int option =
    // "<release>-stellar-core-<i>-0" -> i
    let parts = podName.Split('-')

    if parts.Length < 2 then
        None
    else
        match Int32.TryParse(parts.[parts.Length - 2]) with
        | true, i -> Some i
        | _ -> None

// Run a shell command in the first worker that answers, returning whether any did.
//
// `liveWorkers` is what the driver asked helm to render, not what is running. A
// Pending pod is not exec-able, and betting on the lowest index is what silently
// disabled every scale-down pass of a 40-worker run once worker 0 could not be
// scheduled: the exec failed the websocket upgrade, the exception unwound the
// whole pass, and the log still claimed workers were being marked. Prefer pods
// that have heartbeated, then fall back to any live index to break the
// chicken-and-egg on the very first read.
let execInAnyWorker (context: MissionContext) (preferred: Set<int>) (cmd: string) (outFile: string option) : bool =
    let candidates =
        Seq.append (Seq.sort preferred) (liveWorkers |> Seq.sort |> Seq.filter (preferred.Contains >> not))

    candidates
    |> Seq.exists
        (fun i ->
            try
                match outFile with
                | Some path ->
                    RemoteCommandRunner.RunRemoteCommandAndCaptureOutput(
                        kube = context.kube,
                        ns = context.namespaceProperty,
                        podName = workerPodName i,
                        containerName = "stellar-core",
                        command = [| "sh"; "-c"; cmd |],
                        outputFilePath = path
                    )

                    true
                | None ->
                    RemoteCommandRunner.RunRemoteCommand(
                        context.kube,
                        context.namespaceProperty,
                        workerPodName i,
                        "stellar-core",
                        cmd + "\nexit $?\n"
                    ) = 0
            with _ -> false)

let private readPodSet (context: MissionContext) (preferred: Set<int>) (name: string) (cmd: string) : Set<string> =
    let outFile = Path.Combine(Path.GetTempPath(), sprintf "%s-%s.txt" helmReleaseName name)

    if execInAnyWorker context preferred cmd (Some outFile) then
        File.ReadAllLines outFile
        |> Seq.map (fun line -> line.Trim())
        |> Seq.filter (fun line -> line <> "")
        |> Set.ofSeq
    else
        LogWarn "Could not read %s: no worker answered" name
        Set.empty

// Workers that beat recently, so are provably running worker.sh. The cutoff is
// computed with the pod's own clock, because the driver runs outside the cluster
// and its clock cannot be assumed to agree.
let readyWorkers (context: MissionContext) : Set<int> =
    let cmd =
        sprintf
            "redis-cli -h \"$REDIS_HOST\" -p \"$REDIS_PORT\" ZRANGEBYSCORE \"$RELEASE_NAME-ready\" $(( $(date +%%s) - %d )) +inf"
            readyStaleSecs

    readPodSet context Set.empty "ready" cmd
    |> Set.map indexOfPodName
    |> Set.filter Option.isSome
    |> Set.map Option.get

// Pods that have announced they saw the retiring mark while between jobs. This,
// not the status snapshot, is what authorises a deletion.
let retiredPods (context: MissionContext) (preferred: Set<int>) : Set<string> =
    readPodSet
        context
        preferred
        "retired"
        "redis-cli -h \"$REDIS_HOST\" -p \"$REDIS_PORT\" SMEMBERS \"$RELEASE_NAME-retired\""

// Add pods to the retiring set that worker.sh checks before each claim. Returns
// the pods actually committed: a chunk that fails partway through must not discard
// the record of the chunks that already landed, or the driver's view and Redis
// diverge and those pods stop claiming with nothing tracking them. An exec that
// throws counts as a failed chunk, not as an aborted pass.
let markRetiring (context: MissionContext) (preferred: Set<int>) (podNames: string list) : string list =
    let committed = ResizeArray<string>()
    let mutable stop = false

    for chunk in podNames |> List.chunkBySize 30 do
        if not stop then
            let args = chunk |> List.map (sprintf "'%s'") |> String.concat " "

            let cmd =
                sprintf "redis-cli -h \"$REDIS_HOST\" -p \"$REDIS_PORT\" SADD \"$RELEASE_NAME-retiring\" %s" args

            if execInAnyWorker context preferred cmd None then
                committed.AddRange chunk
            else
                LogWarn "Marking workers retiring failed; %d of %d committed" committed.Count podNames.Length
                stop <- true

    List.ofSeq committed

// Highest index first, so the fleet shrinks from the top and the surviving
// workers stay contiguous from 0.
let private selectWorkers (candidates: Set<int>) (limit: int) (predicate: string -> bool) : int list =
    candidates
    |> Seq.sortDescending
    |> Seq.filter (workerPodName >> predicate)
    |> Seq.truncate limit
    |> List.ofSeq

// The workers to mark retiring this pass: idle, not already marked, and only as
// many as the outstanding work lets us give up.
//
// The floor is `queued + inProgress`, not `queued` alone. With N jobs queued and
// every other worker busy, a floor of N would mark workers the queue still needs
// and leave those N jobs unclaimable.
let workersToMark
    (ready: Set<int>)
    (owningPods: Set<string>)
    (marked: Set<string>)
    (queued: int)
    (inProgress: int)
    : int list =
    let surplus = ready.Count - (queued + inProgress)

    if surplus <= 0 then
        []
    else
        selectWorkers ready surplus (fun pod -> not (owningPods.Contains pod) && not (marked.Contains pod))

// The workers that can be deleted now: live, and announced as retired by the worker
// itself.
//
// The announcement is the entire safety property, and it has to come from the worker
// rather than from the status snapshot. The monitor reads job ownership at the start
// of its cycle and republishes only after serially pinging every owner, so a snapshot
// the driver reads can be older than the mark that preceded it. Two consecutive polls
// can see the same stale snapshot, and a worker that claimed a range in between reads
// as idle in both. Judging idleness that way deletes a worker mid-range, and the
// monitor requeues in-progress work only when EVERY owner is unreachable -- so that
// range is never retried and the mission never finishes draining. A pod in the retired
// set has told us it reached the top of its claim loop with the mark already set: a
// fact about the worker, not an inference about a snapshot's age.
let workersToRemove (live: Set<int>) (acked: Set<string>) : int list =
    selectWorkers live maxWorkersRetiredPerPass acked.Contains

// For each index's pod, tars every "stellar-core-*.log" in /data and copies
// the archive into context.destination.
// Returns the indices whose collection attempt raised. An empty archive is not
// a failure: a worker that never claimed a job has no logs to lose.
let collectLogsFromIndices (context: MissionContext) (indices: int list) : int list =
    LogInfo "Collecting logs from %d worker pods to directory: %s" (List.length indices) context.destination.Path

    let mutable failedIndices = []

    for index in indices do
        let podName = workerPodName index

        try
            LogInfo "Collecting logs from pod: %s" podName

            // Build the tar command to archive log files
            // The command tars all stellar-core-*.log files in /data
            // Using `-f -` to write the file contents to stdout
            let command = [| "sh"; "-c"; "cd /data && tar -czf - stellar-core-*.log" |]

            // Output file path for this pod's logs
            let outputFile = Path.Combine(context.destination.Path, sprintf "%s-logs.tar.gz" podName)

            // Execute the command and capture the tar output to a local file
            RemoteCommandRunner.RunRemoteCommandAndCaptureOutput(
                kube = context.kube,
                ns = context.namespaceProperty,
                podName = podName,
                containerName = "stellar-core",
                command = command,
                outputFilePath = outputFile
            )

            let fileInfo = FileInfo(outputFile)

            if fileInfo.Exists && fileInfo.Length > 0L then
                LogInfo "Successfully collected logs from %s to %s (size: %d bytes)" podName outputFile fileInfo.Length
            else
                LogWarn "No logs found or empty archive for pod %s" podName

        with ex ->
            LogWarn "Could not collect logs from pod %s (this is expected if pod doesn't exist): %s" podName ex.Message
            failedIndices <- index :: failedIndices

    failedIndices

// Collect from every worker the run was sized for. Retiring workers are
// collected earlier, as they are removed.
let collectLogsFromPods (context: MissionContext) = collectLogsFromIndices context (List.ofSeq liveWorkers) |> ignore

// Cleanup on exit. `signalTriggered` indicates we're running under a hard
// deadline (Jenkins' SoftKillWaitSeconds, ~5s by default, before SIGKILL).
// In that case we have to prioritize getting `helm uninstall` issued ahead
// of the much-slower log collection — otherwise we get SIGKILLed mid-
// collection and leak every worker pod, which is what we saw in practice
// with a 1024-worker run aborted from Jenkins.
let cleanup (signalTriggered: bool) (context: MissionContext) =
    if toPerformCleanup then
        toPerformCleanup <- false

        if signalTriggered then
            // Abort path: resources first, logs are nice-to-have.
            // Skip log collection entirely — even parallelized it can't beat
            // Jenkins' ~5s grace before SIGKILL, and it can't beat the per-pod
            // terminationGracePeriodSeconds (default 30s) when scaled to 1024
            // workers. Whatever logs were captured inline by the failure
            // handler in the main loop are still on disk.
            LogInfo "Signal-triggered cleanup: uninstalling release %s" helmReleaseName

            RunShellCommand [| "helm"
                               "uninstall"
                               helmReleaseName |]
            |> ignore
        else
            // Normal / legitimate-failure path: pods are still alive through
            // this entire window, so we can collect all logs before deleting.
            LogInfo "Cleaning up resources for release: %s" helmReleaseName

            try
                LogInfo "Attempting to collect worker logs before cleanup..."
                let stopwatch = Stopwatch.StartNew()
                collectLogsFromPods context
                stopwatch.Stop()
                LogInfo "Log collection completed in %.2f seconds" stopwatch.Elapsed.TotalSeconds
            with ex -> LogWarn "Failed to collect some or all worker logs: %s" ex.Message

            RunShellCommand [| "helm"
                               "uninstall"
                               helmReleaseName |]
            |> ignore

let mutable cleanupContext : MissionContext option = None

// NOTE: AppDomain.ProcessExit handlers have a soft ~2-second runtime budget
// before .NET force-exits the process. If we ever observe that this budget is insufficient, switch to
// `PosixSignalRegistration.Create(PosixSignal.SIGTERM, ...)`
// which has no such budget and lets the handler run to completion within
// Jenkins' full SoftKillWaitSeconds window (~5s default).
System.AppDomain.CurrentDomain.ProcessExit.Add
    (fun _ ->
        match cleanupContext with
        | Some ctx -> cleanup true ctx
        | None -> ())

Console.CancelKeyPress.Add
    (fun _ ->
        match cleanupContext with
        | Some ctx -> cleanup true ctx
        | None -> ()

        Environment.Exit(0))

let queryJobMonitor (context: MissionContext, path: String, endPoint: String) =
    try
        use client = new HttpClient()
        let url = "http://" + jobMonitorHostName context + path + endPoint
        let response = client.GetStringAsync(url).Result

        LogInfo "job monitor query '%s', got response: %s" url response
        let json = JObject.Parse(response)
        Some(json)
    with ex ->
        LogError "Error querying job monitor '%s': %s" endPoint ex.Message
        None


let dumpLogs (context: MissionContext, podName: String) =
    let stream =
        context.kube.ReadNamespacedPodLog(
            name = podName,
            namespaceParameter = context.namespaceProperty,
            container = "stellar-core",
            tailLines = Nullable<int> failedJobLogFileLineCount // lines to log to the file
        )
    // log the last few lines to the concole
    use reader = new System.IO.StreamReader(stream)
    let logLines = ResizeArray<string>()

    while not reader.EndOfStream do
        logLines.Add(reader.ReadLine())

    let lineStart = max 0 (logLines.Count - failedJobLogStreamLineCount)

    for i in lineStart .. logLines.Count - 1 do
        LogInfo "%s" logLines.[i]

    let filename = sprintf "FAILED-last%dlines-%s.log" failedJobLogFileLineCount podName
    context.destination.WriteLines filename (logLines.ToArray())
    stream.Close()

let historyPubnetParallelCatchupV2 (context: MissionContext) =
    LogInfo "Running parallel catchup v2 ..."

    nonce <- (MakeNetworkNonce context.tag).ToString()
    helmReleaseName <- sprintf "parallel-catchup-%s" nonce
    LogDebug "nonce: '%s', release name: '%s'" nonce helmReleaseName

    // Set cleanup context so cleanup handlers can access it
    cleanupContext <- Some context

    installProject context

    let mutable allJobsFinished = false
    liveWorkers <- Set.ofList [ 0 .. context.pubnetParallelCatchupNumWorkers - 1 ]
    // Pods we have already asked to retire, kept only so the same mark is not
    // re-issued every pass. Not a safety mechanism -- the retired set is. Marking is
    // never reversed even though the monitor's stuck-job path can return in-progress
    // work to the queue, so the queue does not strictly drain. The cost of that is a
    // smaller fleet for the rest of the run, never a stall, because marking always
    // leaves queued + in-progress workers unmarked.
    let mutable markedPods : Set<string> = Set.empty
    let mutable timeoutLeft = jobMonitorStatusCheckTimeOutSecs
    let mutable timeBeforeNextMetricsCheck = jobMonitorMetricsCheckIntervalSecs
    let jobMonitorPath = "/" + context.namespaceProperty + "/" + helmReleaseName

    while not allJobsFinished do
        Thread.Sleep(jobMonitorStatusCheckIntervalSecs * 1000)
        let statusOpt = queryJobMonitor (context, jobMonitorPath, jobMonitorStatusEndPoint)

        try
            match statusOpt with
            | Some status ->
                timeoutLeft <- jobMonitorStatusCheckTimeOutSecs
                let remainSize = status.Value<int>("num_remain")
                let jobsFailed = status.["jobs_failed"] :?> JArray
                let JobsInProgress = status.["jobs_in_progress"] :?> JArray

                if jobsFailed.Count <> 0 then
                    LogInfo "One or more jobs have failed:"

                    for job in jobsFailed do
                        let ident = job.ToString().Split('|')
                        let key = ident.[0]
                        let podName = ident.[1]
                        LogInfo "%s, logs >>> " (job.ToString())
                        dumpLogs (context, podName)
                        LogInfo "<<<"

                    failwith "Catch up failed, check logs for more info"

                // Retire the workers the queue can no longer keep busy, in two
                // phases one poll apart: mark, then delete what was marked
                // earlier. A failed pass is not fatal -- skip it and retry on
                // the next poll rather than taking a multi-hour catchup down
                // over a transient error.
                //
                // `queue_remain_count`, not `num_remain`: the two are equal in
                // every published status, but the monitor's pre-first-poll
                // placeholder carries `num_remain = 1` as a "something is
                // running" sentinel. Read that as a queue depth and the very
                // first poll retires the whole fleet.
                let queuedCount = status.Value<int>("queue_remain_count")

                if (queuedCount > 0 || JobsInProgress.Count > 0) && not liveWorkers.IsEmpty then
                    try
                        let owningPods =
                            (status.["workers"] :?> JArray)
                            |> Seq.map (fun w -> w.Value<string>("pod"))
                            |> Set.ofSeq

                        // Only workers that beat recently count as capacity or can
                        // host a command. A Pending pod does neither, and counting it
                        // as spare capacity marks workers the queue still needs, then
                        // retires them the moment they schedule.
                        let ready = readyWorkers context

                        let acked = if markedPods.IsEmpty then Set.empty else retiredPods context ready

                        let removable = workersToRemove liveWorkers acked

                        if not removable.IsEmpty then
                            LogInfo
                                "Removing retired workers %A (%d live, %d ready, %d queued, %d in progress)"
                                removable
                                liveWorkers.Count
                                ready.Count
                                queuedCount
                                JobsInProgress.Count

                            // Collect before deleting: /data is emptyDir, so a pod
                            // deleted before its logs are read loses them for good.
                            // Only the workers whose collection failed are held back;
                            // failing the whole batch on one of them lets a single pod
                            // stall every removal for the rest of the run.
                            let failed = collectLogsFromIndices context removable

                            for index in removable |> List.filter (fun i -> not (List.contains i failed)) do
                                try
                                    context.kube.DeleteNamespacedStatefulSet(
                                        workerStatefulSetName index,
                                        context.namespaceProperty
                                    )
                                    |> ignore

                                    // Per worker, not once per batch: a throw partway
                                    // through would otherwise leave already-deleted
                                    // workers recorded as live, and every later pass
                                    // would re-select them and fail collecting from a
                                    // pod that no longer exists.
                                    liveWorkers <- Set.remove index liveWorkers
                                with
                                | :? Microsoft.Rest.HttpOperationException as ex when
                                    ex.Response.StatusCode = HttpStatusCode.NotFound ->
                                    // Already gone: nothing to delete, and no logs
                                    // left to lose. Leaving it in liveWorkers would
                                    // re-select it every pass and fail collecting
                                    // from a pod that no longer exists.
                                    LogInfo "Worker %d was already gone" index
                                    liveWorkers <- Set.remove index liveWorkers
                                | ex -> LogWarn "Could not remove worker %d: %s" index ex.Message

                            if not failed.IsEmpty then
                                LogWarn
                                    "Held back %d of %d workers: log collection failed"
                                    failed.Length
                                    removable.Length

                        let toMark = workersToMark ready owningPods markedPods queuedCount JobsInProgress.Count

                        if not toMark.IsEmpty then
                            // Logged after the fact with the committed count, not
                            // before with the intended one: a run once logged seven
                            // marking waves having marked nothing at all.
                            let committed = markRetiring context ready (toMark |> List.map workerPodName)
                            markedPods <- Set.union markedPods (Set.ofList committed)

                            LogInfo
                                "Marked %d of %d workers retiring (%d ready)"
                                committed.Length
                                toMark.Length
                                ready.Count
                    with ex -> LogWarn "Worker scale-down skipped this pass: %s" ex.Message

                if remainSize = 0 && JobsInProgress.Count = 0 then
                    // All jobs completed — perform a final query on the metrics
                    queryJobMonitor (context, jobMonitorPath, jobMonitorMetricsEndPoint) |> ignore
                    LogInfo "All queues empty. Mission complete."
                    allJobsFinished <- true

                // check the metrics
                timeBeforeNextMetricsCheck <- timeBeforeNextMetricsCheck - jobMonitorStatusCheckIntervalSecs

                if timeBeforeNextMetricsCheck <= 0 then
                    queryJobMonitor (context, jobMonitorPath, jobMonitorMetricsEndPoint) |> ignore
                    timeBeforeNextMetricsCheck <- jobMonitorMetricsCheckIntervalSecs

            | None ->
                LogError "no status"
                timeoutLeft <- timeoutLeft - jobMonitorStatusCheckIntervalSecs
                if timeoutLeft <= 0 then failwith "job monitor not reachable"
        with ex ->
            cleanup false context
            raise ex

    cleanup false context
