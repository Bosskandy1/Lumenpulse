"""
Background job queue for long-running analytics work (Issue #1248).

:class:`JobQueue` ties a :class:`~src.jobs.job_store.JobStore` to ``asyncio`` so
that a long-running operation can be *submitted* and immediately acknowledged
(HTTP 202) while its work runs in the background on the event loop. The queue:

* Creates/collapses the job via the store (duplicate submissions for identical
  parameters return the existing active job rather than starting a second run).
* Schedules the job function with :func:`asyncio.create_task`, so the HTTP
  handler returns without blocking.
* Drives the status transitions ``queued -> running -> succeeded|failed`` and
  stores the result reference.
* Keeps a heartbeat ticking while the job runs so that, if the process is
  restarted mid-flight, the store can later detect and report the loss.

Both synchronous and ``async`` job functions are supported: synchronous
functions (the common case here - they call into CPU/IO-bound analytics code)
are run in the default thread-pool executor so the event loop is never blocked.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
from typing import Any, Callable, Optional, Set

from src.jobs.job_store import Job, JobStore

logger = logging.getLogger(__name__)


def _default_heartbeat_interval() -> float:
    """Read the heartbeat cadence (seconds) from the environment."""
    try:
        return float(os.getenv("JOB_HEARTBEAT_INTERVAL", "5"))
    except (TypeError, ValueError):
        return 5.0


class JobQueue:
    """
    Submit-and-forget job runner backed by a :class:`JobStore`.

    Args:
        store: The job store used for persistence and lifecycle updates.
        heartbeat_interval: Seconds between heartbeat updates while a job runs.
    """

    def __init__(
        self,
        store: JobStore,
        heartbeat_interval: Optional[float] = None,
    ) -> None:
        self.store = store
        self.heartbeat_interval = (
            _default_heartbeat_interval()
            if heartbeat_interval is None
            else float(heartbeat_interval)
        )
        # Keep strong references so background tasks are not garbage-collected.
        self._tasks: Set["asyncio.Task[Any]"] = set()

    async def submit(
        self,
        job_type: str,
        params: Any,
        func: Callable[[], Any],
    ) -> Job:
        """
        Submit ``func`` to run in the background and return the job record.

        If an identical submission (same ``job_type`` and ``params``) is already
        queued or running, that existing job is returned and ``func`` is not
        scheduled a second time (duplicate collapse).

        Args:
            job_type: Logical job category (e.g. ``"retrain"``).
            params: JSON-serialisable parameters; also used to derive the dedupe
                key. These should fully determine the work performed.
            func: A zero-argument callable (sync or async) performing the work
                and returning a JSON-serialisable result.

        Returns:
            The :class:`Job` record, already persisted, in ``queued`` (new) or
            ``queued``/``running`` (collapsed) state.
        """
        job, created = self.store.create(job_type, params)
        if not created:
            logger.info(
                "Duplicate submission collapsed onto job %s (type=%s, status=%s)",
                job.id,
                job.type,
                job.status,
            )
            return job

        task = asyncio.create_task(self._execute(job.id, func))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return job

    async def _execute(self, job_id: str, func: Callable[[], Any]) -> None:
        """Run the job function, driving status and heartbeat transitions."""
        self.store.mark_running(job_id)
        heartbeat_task = asyncio.create_task(self._heartbeat(job_id))
        try:
            if inspect.iscoroutinefunction(func):
                result = await func()
            else:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, func)
            self.store.mark_succeeded(job_id, result=result)
            logger.info("Job %s succeeded", job_id)
        except Exception as exc:  # noqa: BLE001 - failures must be recorded, not raised
            logger.exception("Job %s failed", job_id)
            self.store.mark_failed(job_id, str(exc))
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

    async def _heartbeat(self, job_id: str) -> None:
        """Periodically refresh the running job's heartbeat until cancelled."""
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval)
                self.store.heartbeat(job_id)
        except asyncio.CancelledError:  # pragma: no cover - normal shutdown path
            raise
