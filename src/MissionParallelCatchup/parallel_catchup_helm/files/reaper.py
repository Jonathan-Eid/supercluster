#!/usr/bin/env python3
"""Idle-worker reaper for parallel catchup.

At the tail of a run the job queue drains but the StatefulSet still holds every
worker, so ~1000 pods idle for hours waiting on a handful of stragglers (37.7%
of measured worker-hours). A StatefulSet cannot drop an arbitrary ordinal --
scaling only removes contiguous top ordinals, and deleting a pod just recreates
it -- so instead we make an idle ordinal permanently unschedulable:

    delete its PVC -> delete its pod -> recreate the PVC pre-bound to a
    PersistentVolume that does not exist

The PVC can never bind, so the pod can never schedule, so Karpenter sees the
node as empty and reaps it (verified: `reason="empty" pod-count=0`). The EBS
volume is released by the CSI driver through the normal delete path.

Two invariants this file must never violate:

  1. Never patch a finalizer, and never touch a PersistentVolume. Reclaiming
     EBS is the CSI driver's job; stepping outside that path orphans volumes
     that nothing in the cluster can clean up. The RBAC role deliberately
     grants no PV access and no patch verb so this is enforced, not just
     documented.
  2. Never neuter a worker that owns a job. See the reap gate below.
"""

import logging
import os
import signal
import sys
import time

import redis
from kubernetes import client, config
from kubernetes.client.rest import ApiException

log = logging.getLogger("reaper")


def env(name, default=None, cast=str):
    raw = os.environ.get(name, default)
    if raw is None:
        raise SystemExit(f"missing required env var {name}")
    return cast(raw)


class Config:
    def __init__(self):
        self.namespace = env("NAMESPACE")
        self.sts_name = env("STS_NAME")
        self.volume_name = env("VOLUME_NAME", "data-volume")
        self.redis_host = env("REDIS_HOST")
        self.redis_port = env("REDIS_PORT", "6379", int)
        self.job_queue = env("JOB_QUEUE", "ranges")
        self.progress_queue = env("PROGRESS_QUEUE", "in_progress")
        self.job_owners = env("JOB_OWNERS", "job_owners")
        self.min_workers = env("MIN_WORKERS", "8", int)
        self.confirm_interval = env("CONFIRM_INTERVAL_SECONDS", "30", int)
        self.poll_interval = env("POLL_INTERVAL_SECONDS", "30", int)
        self.max_reaps_per_cycle = env("MAX_REAPS_PER_CYCLE", "64", int)
        self.dry_run = env("DRY_RUN", "true").lower() == "true"


# A PV name that is never created. A PVC naming it stays Pending forever.
HOLD_VOLUME = "reaper-neutered-no-such-pv"
HOLD_LABEL = "reaper"
HOLD_LABEL_VALUE = "held"

# Single round trip so the four queues cannot shift relative to each other
# between reads. Redis is single-threaded, so this is a consistent snapshot.
SNAPSHOT_LUA = """
local out = {}
out[#out+1] = tostring(redis.call('LLEN', KEYS[1]))
local ip = redis.call('LRANGE', KEYS[2], 0, -1)
out[#out+1] = tostring(#ip)
for _, v in ipairs(ip) do out[#out+1] = 'P' .. v end
local ow = redis.call('HGETALL', KEYS[3])
for i = 1, #ow, 2 do out[#out+1] = 'O' .. ow[i] .. '\\t' .. ow[i + 1] end
return out
"""


class Snapshot:
    def __init__(self, remaining, in_progress, owners):
        self.remaining = remaining
        self.in_progress = in_progress
        self.owners = owners

    @property
    def busy_pods(self):
        return set(self.owners.values())

    @property
    def consistent(self):
        """G2: every in-flight job has a recorded owner and vice versa.

        worker.sh claims a job and records ownership in one atomic EVAL, so a
        mismatch means something is mid-update (e.g. the OOM handler) and the
        ownership view cannot be trusted this cycle.
        """
        return set(self.owners.keys()) == self.in_progress

    @property
    def drained(self):
        """G1: no claimable work left.

        This is the keystone. With the queue empty no idle worker can *become*
        busy, so the idle set is stable and a reap decision cannot go stale
        between observing it and acting on it.
        """
        return self.remaining == 0


def take_snapshot(r, cfg):
    raw = r.eval(SNAPSHOT_LUA, 3, cfg.job_queue, cfg.progress_queue, cfg.job_owners)
    vals = [v.decode() if isinstance(v, bytes) else str(v) for v in raw]
    remaining = int(vals[0])
    in_progress, owners = set(), {}
    for item in vals[2:]:
        if item.startswith("P"):
            in_progress.add(item[1:])
        elif item.startswith("O"):
            job, _, pod = item[1:].partition("\t")
            owners[job] = pod
    return Snapshot(remaining, in_progress, owners)


class Reaper:
    def __init__(self, cfg, core, apps):
        self.cfg = cfg
        self.core = core
        self.apps = apps
        self.sts_uid = None
        self.storage_size = None
        self.storage_class = None
        self.idle_since = {}

    def preflight(self):
        """Fail closed. Any unmet precondition means we never reap at all."""
        sts = self.apps.read_namespaced_stateful_set(self.cfg.sts_name, self.cfg.namespace)
        self.sts_uid = sts.metadata.uid

        policy = sts.spec.persistent_volume_claim_retention_policy
        if not policy or policy.when_scaled != "Delete" or policy.when_deleted != "Delete":
            raise SystemExit(
                "refusing to run: StatefulSet lacks persistentVolumeClaimRetentionPolicy "
                "{whenScaled: Delete, whenDeleted: Delete}; PVCs would outlive the run and leak EBS"
            )

        templates = sts.spec.volume_claim_templates or []
        tmpl = next((t for t in templates if t.metadata.name == self.cfg.volume_name), None)
        if tmpl is None:
            raise SystemExit(f"refusing to run: no volumeClaimTemplate named {self.cfg.volume_name}")
        self.storage_size = tmpl.spec.resources.requests["storage"]
        self.storage_class = tmpl.spec.storage_class_name

        sc = client.StorageV1Api().read_storage_class(self.storage_class)
        # Under Immediate binding a hold PVC would provision a real EBS volume
        # instead of staying Pending -- the reaper would create cost and leaks
        # rather than remove them.
        if sc.volume_binding_mode != "WaitForFirstConsumer":
            raise SystemExit(
                f"refusing to run: storageclass {self.storage_class} is "
                f"{sc.volume_binding_mode}, need WaitForFirstConsumer"
            )
        if sc.reclaim_policy != "Delete":
            raise SystemExit(
                f"refusing to run: storageclass {self.storage_class} reclaimPolicy is "
                f"{sc.reclaim_policy}, need Delete or EBS volumes are never reclaimed"
            )
        log.info(
            "preflight ok: sts=%s size=%s sc=%s (WaitForFirstConsumer, Delete) dry_run=%s",
            self.cfg.sts_name, self.storage_size, self.storage_class, self.cfg.dry_run,
        )

    def pvc_name(self, pod_name):
        return f"{self.cfg.volume_name}-{pod_name}"

    def list_worker_pods(self):
        return self.core.list_namespaced_pod(
            self.cfg.namespace, label_selector=f"app={self.cfg.sts_name}"
        ).items

    def held_pvcs(self):
        return self.core.list_namespaced_persistent_volume_claim(
            self.cfg.namespace, label_selector=f"{HOLD_LABEL}={HOLD_LABEL_VALUE}"
        ).items

    def hold_pvc_body(self, name):
        return client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=self.cfg.namespace,
                labels={HOLD_LABEL: HOLD_LABEL_VALUE},
                # Owned by the StatefulSet so `helm uninstall` garbage-collects
                # the hold PVCs even if this reaper died mid-run.
                owner_references=[
                    client.V1OwnerReference(
                        api_version="apps/v1",
                        kind="StatefulSet",
                        name=self.cfg.sts_name,
                        uid=self.sts_uid,
                        controller=False,
                        block_owner_deletion=False,
                    )
                ],
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                storage_class_name=self.storage_class,
                volume_name=HOLD_VOLUME,
                resources=client.V1VolumeResourceRequirements(
                    requests={"storage": self.storage_size}
                ),
            ),
        )

    def neuter(self, pod_name):
        pvc = self.pvc_name(pod_name)
        if self.cfg.dry_run:
            log.info("[dry-run] would neuter %s (pvc %s)", pod_name, pvc)
            return True

        # Ordinary deletes only. The PVC blocks on pvc-protection until the pod
        # is gone; that is correct, let the finalizer run.
        self._delete_pvc(pvc)
        self._delete_pod(pod_name)

        # The StatefulSet controller recreates the PVC from its template within
        # about a second of recreating the pod, so we contend for the name. A
        # lost round is free: under WaitForFirstConsumer a PVC whose pod cannot
        # schedule provisions no EBS.
        for _ in range(40):
            existing = self._get_pvc(pvc)
            if existing is None:
                try:
                    self.core.create_namespaced_persistent_volume_claim(
                        self.cfg.namespace, self.hold_pvc_body(pvc)
                    )
                    log.info("neutered %s: hold PVC %s created", pod_name, pvc)
                    return True
                except ApiException as e:
                    if e.status != 409:
                        raise
            elif (existing.metadata.labels or {}).get(HOLD_LABEL) == HOLD_LABEL_VALUE:
                return True
            elif existing.metadata.deletion_timestamp is None and existing.status.phase == "Pending":
                self._delete_pvc(pvc)
            time.sleep(1)

        log.warning("gave up installing hold PVC for %s", pod_name)
        return False

    def unneuter(self, pvc_name):
        pod_name = pvc_name[len(self.cfg.volume_name) + 1:]
        if self.cfg.dry_run:
            log.info("[dry-run] would restore %s", pod_name)
            return
        # The hold PVC is Pending with no volume behind it, so this is instant
        # and cannot leak. Deleting the pod makes the StatefulSet recreate both
        # the pod and a real PVC from its template.
        self._delete_pvc(pvc_name)
        self._delete_pod(pod_name)
        log.info("restored %s", pod_name)

    def _get_pvc(self, name):
        try:
            return self.core.read_namespaced_persistent_volume_claim(name, self.cfg.namespace)
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _delete_pvc(self, name):
        try:
            self.core.delete_namespaced_persistent_volume_claim(name, self.cfg.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    def _delete_pod(self, name):
        try:
            self.core.delete_namespaced_pod(name, self.cfg.namespace)
        except ApiException as e:
            if e.status != 404:
                raise

    def cycle(self, snap, now):
        held = {p.metadata.name for p in self.held_pvcs()}

        # A failed job was requeued: restore capacity before anything else.
        if snap.remaining > 0:
            if held:
                log.info("queue refilled (%d jobs) -- restoring %d held workers",
                         snap.remaining, len(held))
                for name in sorted(held):
                    self.unneuter(name)
            self.idle_since.clear()
            return

        if not snap.drained or not snap.consistent:
            log.info("gate closed: remaining=%d consistent=%s", snap.remaining, snap.consistent)
            self.idle_since.clear()
            return

        busy = snap.busy_pods
        pods = self.list_worker_pods()
        live = [p for p in pods
                if p.status.phase == "Running" and p.metadata.deletion_timestamp is None]
        candidates = [p.metadata.name for p in live if p.metadata.name not in busy]

        # G4: an idle verdict must hold across two observations before we act.
        for name in candidates:
            self.idle_since.setdefault(name, now)
        for name in list(self.idle_since):
            if name not in candidates:
                del self.idle_since[name]
        confirmed = [n for n in candidates
                     if now - self.idle_since[n] >= self.cfg.confirm_interval]

        # G5: keep a floor so a late requeue is not starved.
        floor = max(self.cfg.min_workers, 2 * len(snap.in_progress))
        budget = max(0, len(live) - floor)
        targets = sorted(confirmed)[:min(budget, self.cfg.max_reaps_per_cycle)]

        log.info(
            "live=%d busy=%d idle=%d confirmed=%d floor=%d reaping=%d held=%d",
            len(live), len(busy), len(candidates), len(confirmed), floor, len(targets), len(held),
        )
        for name in targets:
            try:
                self.neuter(name)
            except ApiException as e:
                log.error("failed to neuter %s: %s", name, e)


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout
    )
    cfg = Config()
    config.load_incluster_config()
    reaper = Reaper(cfg, client.CoreV1Api(), client.AppsV1Api())
    reaper.preflight()

    r = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, socket_timeout=10)

    running = {"go": True}

    def stop(*_):
        running["go"] = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    while running["go"]:
        try:
            reaper.cycle(take_snapshot(r, cfg), time.monotonic())
        except Exception:
            # Never take the mission down: no reaping is always an acceptable
            # outcome, a crashed reaper mid-neuter is not.
            log.exception("cycle failed")
        time.sleep(cfg.poll_interval)


if __name__ == "__main__":
    main()
