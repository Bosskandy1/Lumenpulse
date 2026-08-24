"""
Tests for Daily On-Chain KPI Snapshot Scheduler (#877)
"""

import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient

from src.db.postgres_service import PostgresService
from src.db.models import (
    Base,
    ProjectView,
    ContractEvent,
    DailyOnchainKPISnapshot,
)
from src.analytics.daily_kpi_snapshot import (
    DailyKPISnapshotGenerator,
    KPISnapshotMetrics,
)
from src.scheduler import _daily_onchain_kpi_snapshot_job, AnalyticsScheduler
from src.api.server import app


@pytest.fixture
def sqlite_db_service(tmp_path):
    """Provide an in-memory SQLite PostgresService for testing."""
    db_path = tmp_path / "test_kpi_snapshots.db"
    db_url = f"sqlite:///{db_path}"
    service = PostgresService(database_url=db_url)
    service.create_tables()
    return service


def test_kpi_snapshot_metrics_dataclass():
    metrics = KPISnapshotMetrics(
        snapshot_date="2026-07-24",
        period="daily",
        tvl=1500.50,
        volume=300.25,
        active_rounds=3,
        contribution_count=12,
        unique_contributors=5,
    )
    d = metrics.to_dict()
    assert d["snapshot_date"] == "2026-07-24"
    assert d["tvl"] == 1500.5
    assert d["volume"] == 300.25
    assert d["active_rounds"] == 3
    assert d["contribution_count"] == 12
    assert d["unique_contributors"] == 5


def test_generator_compute_metrics_and_creation(sqlite_db_service):
    # Populate sample data
    with sqlite_db_service.get_session() as session:
        session.add(
            ProjectView(
                project_id=101,
                total_contributions=1000.0,
                unique_contributors=4,
                status="active",
            )
        )
        session.add(
            ProjectView(
                project_id=102,
                total_contributions=500.0,
                unique_contributors=2,
                status="active",
            )
        )
        session.add(
            ContractEvent(
                contract_id="C123",
                event_id="evt_01",
                ledger=100,
                event_type="depositevent",
                project_id=101,
                contributor="GBX111",
                amount=250.0,
                timestamp=datetime(2026, 7, 24, 12, 0, 0, tzinfo=timezone.utc),
            )
        )

    generator = DailyKPISnapshotGenerator(db_service=sqlite_db_service)
    res = generator.run_snapshot(target_date="2026-07-24", period="daily")

    assert res["status"] == "created"
    assert res["date"] == "2026-07-24"
    assert res["tvl"] == 1500.0
    assert res["active_rounds"] == 2
    assert res["contribution_count"] == 1
    assert res["unique_contributors"] == 1


def test_duplicate_snapshot_skipping(sqlite_db_service):
    generator = DailyKPISnapshotGenerator(db_service=sqlite_db_service)

    # First run: should create snapshot
    res1 = generator.run_snapshot(target_date="2026-07-24", period="daily")
    assert res1["status"] == "created"

    # Second run for same date & period: MUST skip duplicate
    res2 = generator.run_snapshot(target_date="2026-07-24", period="daily")
    assert res2["status"] == "skipped"
    assert "already exists" in res2["message"]


def test_postgres_service_snapshot_methods(sqlite_db_service):
    snapshot_data = {
        "snapshot_date": "2026-07-20",
        "period": "daily",
        "tvl": 2000.0,
        "volume": 500.0,
        "active_rounds": 4,
        "contribution_count": 25,
        "unique_contributors": 10,
    }

    # Test saving snapshot
    s_obj, created = sqlite_db_service.save_daily_onchain_kpi_snapshot(snapshot_data)
    assert created is True
    assert s_obj is not None
    assert s_obj.tvl == 2000.0

    # Test saving duplicate
    s_obj_dup, created_dup = sqlite_db_service.save_daily_onchain_kpi_snapshot(snapshot_data)
    assert created_dup is False
    assert s_obj_dup.id == s_obj.id

    # Test retrieving snapshots
    snapshots = sqlite_db_service.get_daily_onchain_kpi_snapshots(period="daily")
    assert len(snapshots) == 1
    assert snapshots[0].snapshot_date == "2026-07-20"

    # Test retrieving latest snapshot
    latest = sqlite_db_service.get_latest_daily_onchain_kpi_snapshot(period="daily")
    assert latest is not None
    assert latest.snapshot_date == "2026-07-20"


def test_scheduler_job_execution(sqlite_db_service, monkeypatch):
    # Verify that the generator run_snapshot executes properly
    generator = DailyKPISnapshotGenerator(db_service=sqlite_db_service)
    res = generator.run_snapshot(target_date="2026-07-25", period="daily")
    assert res["status"] in ("created", "skipped")

    # Verify scheduled job wrapper function runs cleanly
    _daily_onchain_kpi_snapshot_job()

    # Verify scheduler job configuration
    scheduler = AnalyticsScheduler()
    try:
        scheduler.start()
        job_ids = [job.id for job in scheduler.get_jobs()]
        assert "daily_onchain_kpi_snapshot" in job_ids
    finally:
        scheduler.stop()


def test_api_kpi_snapshot_endpoints(sqlite_db_service, monkeypatch):
    """The snapshot run endpoint is now asynchronous (Issue #1248).

    It returns HTTP 202 with a job id; the caller polls GET /jobs/{id} for the
    terminal result. The context-managed TestClient keeps the event loop alive
    so the background job actually runs to completion.
    """
    import time as _time
    from src.security import security_config
    import src.api.server as server_module

    monkeypatch.setattr(security_config, "api_key", "test-key")
    monkeypatch.setattr(server_module, "postgres_service", sqlite_db_service)

    headers = {"X-API-Key": "test-key"}

    with TestClient(app) as client:
        # Trigger snapshot -> 202 Accepted with a job identifier.
        run_resp = client.post(
            "/analytics/kpis/daily-snapshots/run?target_date=2026-07-24&period=daily",
            headers=headers,
        )
        assert run_resp.status_code == 202
        submission = run_resp.json()
        job_id = submission["job_id"]
        assert submission["status"] in ("queued", "running", "succeeded")
        assert submission["status_url"] == f"/jobs/{job_id}"

        # Poll the job-status endpoint until the run reaches a terminal state.
        job = None
        for _ in range(200):
            status_resp = client.get(f"/jobs/{job_id}", headers=headers)
            assert status_resp.status_code == 200
            body = status_resp.json()
            if body["status"] in ("succeeded", "failed"):
                job = body
                break
            _time.sleep(0.02)

        assert job is not None, "job did not reach a terminal state in time"
        assert job["status"] == "succeeded", job
        assert job["result"]["status"] in ("created", "skipped")
        assert job["result"]["date"] == "2026-07-24"

        # Test GET snapshots list (still synchronous).
        get_resp = client.get("/analytics/kpis/daily-snapshots?period=daily", headers=headers)
        assert get_resp.status_code == 200
        list_data = get_resp.json()
        assert len(list_data) >= 1
        assert list_data[0]["snapshot_date"] == "2026-07-24"
