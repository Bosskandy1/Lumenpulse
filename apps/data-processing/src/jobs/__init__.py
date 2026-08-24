"""
Asynchronous job-queue package for long-running analytics work (Issue #1248).

This package provides a small, dependency-light background job system used to
move long-running data-processing operations (model retraining, correlation
analysis, KPI snapshot generation) off the request/response path.

Public API:
    * :class:`~src.jobs.job_store.JobStore` - pluggable job persistence with a
      Redis backend (survives process restarts) and an in-memory fallback used
      by tests.
    * :class:`~src.jobs.job_store.Job` - a single job record.
    * :data:`~src.jobs.job_store.JobStatus` - the queued/running/succeeded/failed
      status vocabulary.
    * :class:`~src.jobs.job_queue.JobQueue` - submit/collapse/execute helper that
      runs job functions in the background and keeps their status up to date.
"""

from src.jobs.job_store import Job, JobStatus, JobStore
from src.jobs.job_queue import JobQueue

__all__ = ["Job", "JobStatus", "JobStore", "JobQueue"]
