"""
Job store for the asynchronous analytics job queue (Issue #1248).

The :class:`JobStore` persists job records behind a *pluggable* backend:

* A **Redis-backed** backend (used automatically when Redis is configured) so
  that submitted jobs **survive a process restart** - their records live in
  Redis rather than in the FastAPI worker's memory.
* An **in-memory** backend used as a zero-dependency fallback (and by the test
  suite) so nothing external - no Redis, Postgres or network - is required.

Restart-loss detection
----------------------
A durable *record* surviving a restart is not the same as the *work* surviving
it: an in-process worker that was executing a job when the process died cannot
resume. To make that loss visible rather than silent, a running job persists an
incrementing ``heartbeat_at`` timestamp. On store start-up (:meth:`reap_stale`)
and lazily on every status read (:meth:`get`), any job still ``running`` whose
heartbeat is older than ``stale_seconds`` is transitioned to ``failed`` with a
clear "worker lost / process restarted" error. Loss is therefore always
*reported*, never silent (acceptance criterion 3).

Duplicate collapse
------------------
Every job carries a ``dedupe_key`` derived from ``job_type`` plus a SHA-256 of
the canonical JSON of its parameters. :meth:`create` collapses a submission onto
an already-active (queued/running) job with the same key instead of creating a
duplicate (acceptance criterion 4).

The module deliberately relies only on the standard library (``hashlib``,
``json``, ``time``, ``uuid``, ``threading``, ``dataclasses``) plus the ``redis``
client that already ships with this service; no new dependencies are added.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)


class JobStatus:
    """Enumeration of the four job lifecycle states (string constants)."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"

    #: States in which a job is still "active" and can be collapsed onto.
    ACTIVE = (QUEUED, RUNNING)
    #: States in which a job has reached a final outcome.
    TERMINAL = (SUCCEEDED, FAILED)


def _utcnow_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def canonical_json(params: Any) -> str:
    """
    Serialise ``params`` to a deterministic JSON string.

    Keys are sorted and whitespace stripped so that two logically equal
    parameter payloads always produce the same string (and therefore the same
    dedupe key), regardless of dict ordering.
    """
    return json.dumps(
        params, sort_keys=True, separators=(",", ":"), default=str
    )


def compute_dedupe_key(job_type: str, params: Any) -> str:
    """Return ``"{job_type}:{sha256(canonical_json(params))}"``."""
    digest = hashlib.sha256(canonical_json(params).encode("utf-8")).hexdigest()
    return f"{job_type}:{digest}"


@dataclass
class Job:
    """A single background job record."""

    id: str
    type: str
    dedupe_key: str
    status: str = JobStatus.QUEUED
    params: Dict[str, Any] = field(default_factory=dict)
    result_ref: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    owner: Optional[str] = None
    created_at: str = field(default_factory=_utcnow_iso)
    updated_at: str = field(default_factory=_utcnow_iso)
    # Monotonic-ish wall-clock epoch seconds of the last heartbeat while running.
    heartbeat_at: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain ``dict`` representation (JSON-serialisable)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Rebuild a :class:`Job` from a stored ``dict``."""
        known = {f: data.get(f) for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**known)

    def is_active(self) -> bool:
        """Return ``True`` while the job is queued or running."""
        return self.status in JobStatus.ACTIVE

    def is_terminal(self) -> bool:
        """Return ``True`` once the job has succeeded or failed."""
        return self.status in JobStatus.TERMINAL

    def public_dict(self) -> Dict[str, Any]:
        """Return the subset of fields exposed by the status API."""
        return {
            "job_id": self.id,
            "type": self.type,
            "status": self.status,
            "result_ref": self.result_ref,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


class _InMemoryBackend:
    """
    Process-local job backend backed by plain dicts and a re-entrant lock.

    Used by the test suite and as a fallback when Redis is not configured. Job
    records live only for the lifetime of the process, so :meth:`reap_stale`
    will (correctly) surface nothing after a restart - there is simply nothing
    to recover. The Redis backend is what provides cross-restart durability.
    """

    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._dedupe: Dict[str, str] = {}
        self._lock = threading.RLock()

    @contextmanager
    def lock(self) -> Iterator[None]:
        with self._lock:
            yield

    def save(self, job: Dict[str, Any]) -> None:
        self._jobs[job["id"]] = dict(job)

    def load(self, job_id: str) -> Optional[Dict[str, Any]]:
        found = self._jobs.get(job_id)
        return dict(found) if found is not None else None

    def load_all(self) -> List[Dict[str, Any]]:
        return [dict(v) for v in self._jobs.values()]

    def get_dedupe(self, dedupe_key: str) -> Optional[str]:
        return self._dedupe.get(dedupe_key)

    def set_dedupe(self, dedupe_key: str, job_id: str) -> None:
        self._dedupe[dedupe_key] = job_id

    def clear_dedupe(self, dedupe_key: str) -> None:
        self._dedupe.pop(dedupe_key, None)


class _RedisBackend:
    """
    Redis-backed job backend so job records survive a process restart.

    Job records are stored in a single Redis hash (``{namespace}:records``) and
    the active dedupe index in a second hash (``{namespace}:dedupe``). A
    process-local re-entrant lock guards the create/collapse critical section
    against concurrent ``asyncio`` submissions within the same process; the
    hashes themselves provide the cross-restart persistence.
    """

    def __init__(
        self,
        namespace: str = "jobs",
        host: Optional[str] = None,
        port: Optional[int] = None,
        db: Optional[int] = None,
    ) -> None:
        import redis  # imported lazily so the in-memory path needs no server

        self.namespace = namespace
        self._records_key = f"{namespace}:records"
        self._dedupe_key = f"{namespace}:dedupe"
        self._lock = threading.RLock()
        self._client = redis.Redis(
            host=host if host is not None else os.getenv("REDIS_HOST", "localhost"),
            port=port if port is not None else int(os.getenv("REDIS_PORT", "6379")),
            db=db if db is not None else int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        # Fail fast if Redis is unreachable so the caller can fall back.
        self._client.ping()

    @contextmanager
    def lock(self) -> Iterator[None]:
        with self._lock:
            yield

    def save(self, job: Dict[str, Any]) -> None:
        self._client.hset(
            self._records_key, job["id"], json.dumps(job, default=str)
        )

    def load(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = self._client.hget(self._records_key, job_id)
        return json.loads(raw) if raw else None

    def load_all(self) -> List[Dict[str, Any]]:
        raw = self._client.hgetall(self._records_key) or {}
        return [json.loads(v) for v in raw.values()]

    def get_dedupe(self, dedupe_key: str) -> Optional[str]:
        return self._client.hget(self._dedupe_key, dedupe_key)

    def set_dedupe(self, dedupe_key: str, job_id: str) -> None:
        self._client.hset(self._dedupe_key, dedupe_key, job_id)

    def clear_dedupe(self, dedupe_key: str) -> None:
        self._client.hdel(self._dedupe_key, dedupe_key)


def _default_stale_seconds() -> int:
    """Read the stale-heartbeat threshold (seconds) from the environment."""
    try:
        return int(os.getenv("JOB_STALE_SECONDS", "120"))
    except (TypeError, ValueError):
        return 120


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class JobStore:
    """
    Persistence and lifecycle manager for background jobs.

    Args:
        backend: An explicit backend instance. When omitted a backend is chosen
            automatically: Redis when ``JOBS_BACKEND=redis`` (and Redis is
            reachable), otherwise the in-memory fallback.
        stale_seconds: A running job whose heartbeat is older than this many
            seconds is considered lost and transitioned to ``failed``.
        owner: Identifier for this worker/process, recorded on running jobs.
    """

    def __init__(
        self,
        backend: Optional[Any] = None,
        stale_seconds: Optional[int] = None,
        owner: Optional[str] = None,
    ) -> None:
        self.stale_seconds = (
            _default_stale_seconds() if stale_seconds is None else int(stale_seconds)
        )
        self.owner = owner or f"pid:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._backend = backend if backend is not None else self._build_backend()

    # -- backend selection --------------------------------------------------

    @staticmethod
    def _build_backend() -> Any:
        """Select a Redis backend when configured, else the in-memory one."""
        use_redis = os.getenv("JOBS_BACKEND", "").strip().lower() == "redis"
        if use_redis:
            try:
                backend = _RedisBackend()
                logger.info("JobStore using Redis backend (durable across restarts)")
                return backend
            except Exception as exc:  # pragma: no cover - depends on environment
                logger.warning(
                    "JobStore could not connect to Redis (%s); "
                    "falling back to in-memory backend",
                    exc,
                )
        return _InMemoryBackend()

    # -- helpers ------------------------------------------------------------

    def _is_stale(self, job: Job, now: Optional[float] = None) -> bool:
        """Return ``True`` if a running job's heartbeat has gone stale."""
        if job.status != JobStatus.RUNNING:
            return False
        now = time.time() if now is None else now
        return (now - float(job.heartbeat_at or 0.0)) > self.stale_seconds

    def _mark_lost(self, job: Job) -> Job:
        """Transition a stale running job to ``failed`` and persist it."""
        job.status = JobStatus.FAILED
        job.error = (
            "worker lost / process restarted: heartbeat exceeded "
            f"{self.stale_seconds}s stale threshold"
        )
        job.updated_at = _utcnow_iso()
        self._backend.save(job.to_dict())
        self._backend.clear_dedupe(job.dedupe_key)
        logger.warning("Job %s (%s) marked failed: worker lost", job.id, job.type)
        return job

    # -- create / collapse --------------------------------------------------

    def create(self, job_type: str, params: Any) -> Tuple[Job, bool]:
        """
        Create a new job, or collapse onto an existing active duplicate.

        Returns:
            A ``(job, created)`` tuple. ``created`` is ``True`` when a fresh job
            was created and ``False`` when an existing active job with the same
            dedupe key was returned instead.
        """
        dedupe_key = compute_dedupe_key(job_type, params)
        with self._backend.lock():
            existing_id = self._backend.get_dedupe(dedupe_key)
            if existing_id:
                stored = self._backend.load(existing_id)
                if stored is not None:
                    existing = Job.from_dict(stored)
                    if self._is_stale(existing):
                        self._mark_lost(existing)
                    elif existing.is_active():
                        logger.info(
                            "Collapsing duplicate submission for %s onto job %s",
                            job_type,
                            existing.id,
                        )
                        return existing, False
                # Existing job is terminal or stale: clear the stale index.
                self._backend.clear_dedupe(dedupe_key)

            job = Job(
                id=uuid.uuid4().hex,
                type=job_type,
                dedupe_key=dedupe_key,
                status=JobStatus.QUEUED,
                params=dict(params) if isinstance(params, dict) else {"value": params},
                result_ref=None,
            )
            self._backend.save(job.to_dict())
            self._backend.set_dedupe(dedupe_key, job.id)
            logger.info("Created job %s (type=%s)", job.id, job_type)
            return job, True

    # -- reads --------------------------------------------------------------

    def get(self, job_id: str) -> Optional[Job]:
        """
        Return the job, lazily failing it if its heartbeat has gone stale.

        This is where restart loss is surfaced on the read path: a job left
        ``running`` by a since-restarted worker is reported as ``failed`` the
        next time anyone polls its status.
        """
        with self._backend.lock():
            stored = self._backend.load(job_id)
            if stored is None:
                return None
            job = Job.from_dict(stored)
            if self._is_stale(job):
                job = self._mark_lost(job)
            return job

    def list(self) -> List[Job]:
        """Return all known jobs (stale running jobs are reaped first)."""
        self.reap_stale()
        with self._backend.lock():
            return [Job.from_dict(d) for d in self._backend.load_all()]

    # -- updates ------------------------------------------------------------

    def mark_running(self, job_id: str, owner: Optional[str] = None) -> Optional[Job]:
        """Transition a job to ``running`` and record the first heartbeat."""
        with self._backend.lock():
            stored = self._backend.load(job_id)
            if stored is None:
                return None
            job = Job.from_dict(stored)
            job.status = JobStatus.RUNNING
            job.owner = owner or self.owner
            job.heartbeat_at = time.time()
            job.updated_at = _utcnow_iso()
            self._backend.save(job.to_dict())
            return job

    def heartbeat(self, job_id: str) -> Optional[Job]:
        """Bump the running job's heartbeat timestamp."""
        with self._backend.lock():
            stored = self._backend.load(job_id)
            if stored is None:
                return None
            job = Job.from_dict(stored)
            if job.status != JobStatus.RUNNING:
                return job
            job.heartbeat_at = time.time()
            job.updated_at = _utcnow_iso()
            self._backend.save(job.to_dict())
            return job

    def mark_succeeded(
        self,
        job_id: str,
        result: Any = None,
        result_ref: Optional[str] = None,
    ) -> Optional[Job]:
        """Store a successful result and clear the dedupe index."""
        with self._backend.lock():
            stored = self._backend.load(job_id)
            if stored is None:
                return None
            job = Job.from_dict(stored)
            job.status = JobStatus.SUCCEEDED
            job.result = result
            job.result_ref = result_ref or f"/jobs/{job.id}"
            job.error = None
            job.updated_at = _utcnow_iso()
            self._backend.save(job.to_dict())
            self._backend.clear_dedupe(job.dedupe_key)
            return job

    def mark_failed(self, job_id: str, error: str) -> Optional[Job]:
        """Record a failure message and clear the dedupe index."""
        with self._backend.lock():
            stored = self._backend.load(job_id)
            if stored is None:
                return None
            job = Job.from_dict(stored)
            job.status = JobStatus.FAILED
            job.error = str(error)
            job.updated_at = _utcnow_iso()
            self._backend.save(job.to_dict())
            self._backend.clear_dedupe(job.dedupe_key)
            return job

    # -- maintenance --------------------------------------------------------

    def reap_stale(self) -> List[str]:
        """
        Fail every running job whose heartbeat has gone stale.

        Called at start-up so that jobs orphaned by a previous process's crash
        or restart are reported as ``failed`` (never left silently ``running``).

        Returns:
            The ids of the jobs that were transitioned to ``failed``.
        """
        reaped: List[str] = []
        with self._backend.lock():
            now = time.time()
            for stored in self._backend.load_all():
                job = Job.from_dict(stored)
                if self._is_stale(job, now):
                    self._mark_lost(job)
                    reaped.append(job.id)
        if reaped:
            logger.warning(
                "Reaped %d stale job(s) on start-up: %s", len(reaped), reaped
            )
        return reaped
