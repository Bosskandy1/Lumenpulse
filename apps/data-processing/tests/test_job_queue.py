"""
Tests for the asynchronous analytics job queue (Issue #1248).

All tests use the in-memory :class:`JobStore` backend, so no Redis, Postgres or
network is required. They cover:

* the job store: create/get, duplicate collapse, and stale-heartbeat loss
  detection (restart loss reported, never silent);
* the job queue: happy-path success, failure capture, and duplicate collapse of
  a concurrent submission while a job is still running;
* the ``GET /jobs/{job_id}`` status endpoint: terminal status + result
  reference, and 404 for an unknown job.
"""

import asyncio
import os
import time

from src.jobs.job_store import Job, JobStatus, JobStore
from src.jobs.job_queue import JobQueue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _wait_terminal(store, job_id, timeout=5.0):
    """Poll the store until the job is terminal (or the timeout elapses)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = store.get(job_id)
        if job is not None and job.is_terminal():
            # Let the executor's finally-block cancel the heartbeat cleanly.
            await asyncio.sleep(0.02)
            return job
        await asyncio.sleep(0.01)
    return store.get(job_id)


# ---------------------------------------------------------------------------
# Job store
# ---------------------------------------------------------------------------


def test_create_and_get_roundtrip():
    store = JobStore(stale_seconds=60)
    job, created = store.create("demo", {"a": 1})
    assert created is True
    assert job.status == JobStatus.QUEUED

    fetched = store.get(job.id)
    assert fetched is not None
    assert fetched.id == job.id
    assert fetched.type == "demo"
    assert fetched.status == JobStatus.QUEUED


def test_get_unknown_job_returns_none():
    store = JobStore(stale_seconds=60)
    assert store.get("does-not-exist") is None


def test_duplicate_submission_is_collapsed():
    store = JobStore(stale_seconds=60)
    first, created_first = store.create("demo", {"a": 1})
    second, created_second = store.create("demo", {"a": 1})

    assert created_first is True
    assert created_second is False
    assert second.id == first.id  # collapsed onto the active job

    # Different parameters produce a distinct job.
    third, created_third = store.create("demo", {"a": 2})
    assert created_third is True
    assert third.id != first.id


def test_terminal_job_does_not_collapse_new_submission():
    store = JobStore(stale_seconds=60)
    first, _ = store.create("demo", {"a": 1})
    store.mark_running(first.id)
    store.mark_succeeded(first.id, {"ok": True})

    # A new identical submission after completion must create a fresh job.
    second, created = store.create("demo", {"a": 1})
    assert created is True
    assert second.id != first.id


def test_stale_running_job_reported_failed_on_read():
    # stale_seconds=0 => any elapsed time makes a running job stale.
    store = JobStore(stale_seconds=0)
    job, _ = store.create("demo", {"a": 1})
    store.mark_running(job.id)
    time.sleep(0.01)

    fetched = store.get(job.id)
    assert fetched.status == JobStatus.FAILED
    assert "restart" in fetched.error.lower() or "lost" in fetched.error.lower()


def test_reap_stale_marks_running_jobs_failed():
    store = JobStore(stale_seconds=0)
    job, _ = store.create("demo", {"a": 1})
    store.mark_running(job.id)
    time.sleep(0.01)

    reaped = store.reap_stale()
    assert job.id in reaped
    assert store.get(job.id).status == JobStatus.FAILED


def test_queued_job_is_not_considered_stale():
    # A job that never started running must never be reaped as "lost".
    store = JobStore(stale_seconds=0)
    job, _ = store.create("demo", {"a": 1})
    time.sleep(0.01)
    assert store.get(job.id).status == JobStatus.QUEUED
    assert store.reap_stale() == []


def test_job_from_dict_roundtrip():
    job = Job(id="x", type="demo", dedupe_key="demo:abc")
    restored = Job.from_dict(job.to_dict())
    assert restored.id == "x"
    assert restored.type == "demo"
    assert restored.status == JobStatus.QUEUED


# ---------------------------------------------------------------------------
# Job queue
# ---------------------------------------------------------------------------


def test_queue_happy_path_stores_result():
    async def scenario():
        store = JobStore(stale_seconds=60)
        queue = JobQueue(store, heartbeat_interval=0.01)

        def work():
            return {"value": 42}

        job = await queue.submit("demo", {"a": 1}, work)
        assert job.status == JobStatus.QUEUED  # returned immediately

        final = await _wait_terminal(store, job.id)
        assert final.status == JobStatus.SUCCEEDED
        assert final.result == {"value": 42}
        assert final.result_ref == f"/jobs/{job.id}"

    asyncio.run(scenario())


def test_queue_failure_is_captured():
    async def scenario():
        store = JobStore(stale_seconds=60)
        queue = JobQueue(store, heartbeat_interval=0.01)

        def work():
            raise RuntimeError("boom")

        job = await queue.submit("demo", {"a": 1}, work)
        final = await _wait_terminal(store, job.id)
        assert final.status == JobStatus.FAILED
        assert "boom" in (final.error or "")

    asyncio.run(scenario())


def test_queue_supports_async_job_functions():
    async def scenario():
        store = JobStore(stale_seconds=60)
        queue = JobQueue(store, heartbeat_interval=0.01)

        async def work():
            await asyncio.sleep(0.01)
            return {"async": True}

        job = await queue.submit("demo", {"a": 1}, work)
        final = await _wait_terminal(store, job.id)
        assert final.status == JobStatus.SUCCEEDED
        assert final.result == {"async": True}

    asyncio.run(scenario())


def test_queue_collapses_concurrent_duplicate():
    async def scenario():
        store = JobStore(stale_seconds=60)
        queue = JobQueue(store, heartbeat_interval=0.01)

        started = asyncio.Event()
        release = asyncio.Event()

        async def slow():
            started.set()
            await release.wait()
            return {"done": True}

        first = await queue.submit("demo", {"a": 1}, slow)
        await started.wait()  # first job is now running

        second = await queue.submit("demo", {"a": 1}, slow)
        assert second.id == first.id  # collapsed onto the running job

        release.set()
        final = await _wait_terminal(store, first.id)
        assert final.status == JobStatus.SUCCEEDED
        assert final.result == {"done": True}

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------


def test_jobs_status_endpoint_reports_result_and_404():
    from fastapi.testclient import TestClient
    from src.api.server import app, job_store

    job, _ = job_store.create("demo", {"x": 1})
    job_store.mark_running(job.id)
    job_store.mark_succeeded(job.id, {"answer": 42})

    with TestClient(app) as client:
        resp = client.get(f"/jobs/{job.id}", headers={"X-API-Key": "test-key"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["job_id"] == job.id
        assert data["status"] == JobStatus.SUCCEEDED
        assert data["result"] == {"answer": 42}
        assert data["result_ref"] == f"/jobs/{job.id}"

        # Unknown job id -> 404.
        assert client.get("/jobs/does-not-exist", headers={"X-API-Key": "test-key"}).status_code == 404
