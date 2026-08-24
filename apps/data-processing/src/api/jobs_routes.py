"""
Job status API routes for the asynchronous analytics job queue (Issue #1248).

Exposes the polling surface that lets callers (notably the NestJS backend)
track long-running work submitted via the async endpoints:

* ``GET /jobs/{job_id}`` - status of a single job plus its result reference.
* ``GET /jobs`` - list all known jobs (mainly for debugging/operations).

The store is injected by the server module via :func:`configure` to avoid a
circular import between this router and ``src.api.server``. Authentication is
enforced by the shared ``X-API-Key`` middleware installed on the app, exactly
like the other data-processing routes.
"""

from __future__ import annotations

from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.jobs.job_store import JobStore

router = APIRouter(tags=["jobs"])

# Injected at start-up by src.api.server via configure().
_store: Optional[JobStore] = None


def configure(store: JobStore) -> None:
    """Bind the :class:`JobStore` instance used by these routes."""
    global _store
    _store = store


def _require_store() -> JobStore:
    """Return the configured store or raise a 503 if it is missing."""
    if _store is None:  # pragma: no cover - configured at start-up
        raise HTTPException(status_code=503, detail="Job store unavailable")
    return _store


class JobStatusResponse(BaseModel):
    """Status of a single background job."""

    job_id: str = Field(..., description="Unique job identifier")
    type: str = Field(..., description="Logical job type")
    status: str = Field(
        ..., description="One of: queued | running | succeeded | failed"
    )
    result_ref: Optional[str] = Field(
        None, description="Reference (URL) at which the result can be retrieved"
    )
    result: Optional[Any] = Field(
        None, description="Inline result payload once the job has succeeded"
    )
    error: Optional[str] = Field(
        None, description="Error message when the job has failed"
    )
    created_at: str = Field(..., description="Job creation timestamp (ISO-8601)")
    updated_at: str = Field(..., description="Last status change timestamp (ISO-8601)")


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str) -> JobStatusResponse:
    """
    Return the current status and result reference for a single job.

    A job left ``running`` by a since-restarted worker is reported here as
    ``failed`` (with an explanatory error) rather than appearing to hang -
    restart loss is surfaced, never silent.

    Requires the ``X-API-Key`` header (enforced by the app middleware).
    """
    store = _require_store()
    job = store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    return JobStatusResponse(**job.public_dict())


@router.get("/jobs", response_model=List[JobStatusResponse])
async def list_jobs() -> List[JobStatusResponse]:
    """
    Return the status of all known jobs (stale running jobs are reaped first).

    Requires the ``X-API-Key`` header (enforced by the app middleware).
    """
    store = _require_store()
    return [JobStatusResponse(**job.public_dict()) for job in store.list()]
