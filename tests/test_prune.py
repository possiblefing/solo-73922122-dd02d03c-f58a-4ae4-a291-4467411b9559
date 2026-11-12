"""Tests for batched, rebuild-aware change-log pruning."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import SETTINGS


def configure_database(path: Path) -> None:
    object.__setattr__(SETTINGS, "db_path", path)
    object.__setattr__(SETTINGS, "rebuild_batch_delay_ms", 120)
    object.__setattr__(SETTINGS, "rebuild_batch_size", 2)
    object.__setattr__(SETTINGS, "rebuild_lease_seconds", 3)
    object.__setattr__(SETTINGS, "prune_batch_size", 500)


@pytest.fixture
def client(tmp_path: Path):
    configure_database(tmp_path / "archive.db")
    from app.main import app
    from app.rebuild import cancel_local_rebuilds

    with TestClient(app) as test_client:
        yield test_client
    cancel_local_rebuilds()


def _search(client: TestClient, query: str) -> dict:
    response = client.get("/api/search", params={"q": query})
    assert response.status_code == 200, response.text
    return response.json()


def _wait_for_terminal(client: TestClient, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.get("/api/index/status").json()
        job = status["current_rebuild_job"] or status["latest_rebuild_job"]
        if job and job["job_id"] == job_id and job["state"] in {
            "succeeded",
            "failed",
            "interrupted",
        }:
            return status
        time.sleep(0.03)
    raise AssertionError(f"rebuild {job_id} did not finish within {timeout}s")


def _wait_for_base_seq(client: TestClient, timeout: float = 8.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = client.get("/api/index/status").json()
        job = status["current_rebuild_job"]
        if job and job["base_seq"] > 0:
            return job
        time.sleep(0.02)
    raise AssertionError("rebuild did not register a snapshot base sequence")


def test_prune_without_rebuild_empties_log_and_preserves_sequence(client: TestClient):
    revisions = []
    for index in range(3):
        response = client.put(
            f"/api/pages/ocr-{index:03d}",
            json={"title": f"Café {index}", "body": f"Le café numéro {index}."},
        )
        assert response.status_code == 200
        revisions.append(response.json()["revision"])
    destroyed = client.delete("/api/pages/ocr-002")
    assert destroyed.status_code == 200
    revisions.append(destroyed.json()["revision"])
    latest_before = max(revisions)
    assert latest_before == 4

    preview = client.get("/api/changes/prune", params={"upto_seq": 1_000_000})
    assert preview.status_code == 200
    plan = preview.json()
    assert plan["deferred"] is False
    assert plan["deletable_count"] == 4
    assert plan["latest_seq"] == latest_before
    assert plan["retained"]["from_seq"] is None

    pruned = client.post("/api/changes/prune", json={"upto_seq": 1_000_000})
    assert pruned.status_code == 200
    result = pruned.json()
    assert result["deleted_count"] == 4
    assert result["deleted_upto_seq"] == 4
    assert result["completed"] is True
    assert result["remaining_count"] == 0
    # The last committed sequence survives a fully emptied log.
    assert result["latest_seq"] == latest_before
    assert result["vacuum_performed"] is False

    # Search and reads are unaffected by the emptied log.
    assert _search(client, "café")["total"] == 2
    assert client.get("/api/pages/ocr-002").status_code == 404

    # The next commit gets a strictly higher revision: no regression, no reuse.
    again = client.put(
        "/api/pages/ocr-001",
        json={"title": "Café 1 révisé", "body": "Le café corrigé après nettoyage."},
    )
    assert again.status_code == 200
    assert again.json()["revision"] > latest_before

    after = client.get("/api/changes/prune").json()
    assert after["latest_seq"] == again.json()["revision"]
    assert after["total_count"] == 1
    assert after["first_seq"] == again.json()["revision"]


def test_prune_respects_requested_limit_and_is_idempotent(client: TestClient):
    for index in range(6):
        assert client.put(
            f"/api/pages/p-{index}",
            json={"title": f"t{index}", "body": f"mot café {index}"},
        ).status_code == 200

    result = client.post("/api/changes/prune", json={"upto_seq": 4}).json()
    assert result["effective_upto_seq"] == 4
    assert result["deleted_count"] == 4
    assert result["deleted_upto_seq"] == 4
    assert result["remaining_count"] == 2
    assert result["remaining_from_seq"] == 5
    assert result["latest_seq"] == 6

    # Repeating the same call deletes nothing more.
    repeat = client.post("/api/changes/prune", json={"upto_seq": 4}).json()
    assert repeat["deleted_count"] == 0
    assert repeat["completed"] is True
    assert repeat["remaining_count"] == 2

    # Validation rejects a negative limit.
    assert client.post("/api/changes/prune", json={"upto_seq": -1}).status_code == 422


def test_prune_deferred_until_snapshot_base_registered(
    client: TestClient, tmp_path: Path
):
    from app.db import claim_rebuild, fail_job

    assert client.put(
        "/api/pages/a-1", json={"title": "Café", "body": "Le café et le señor."}
    ).status_code == 200

    # Claim a rebuild without starting a worker: it sits in `queued` with no
    # registered snapshot base, so the safe boundary is unknowable.
    claimed = claim_rebuild("fold")
    assert claimed["created"] is True
    job = claimed["job"]
    assert job["base_seq"] == 0

    deferred = client.post("/api/changes/prune", json={"upto_seq": 999_999}).json()
    assert deferred["deferred"] is True
    assert deferred["deleted_count"] == 0
    assert deferred["completed"] is False
    assert deferred["effective_upto_seq"] == 0
    assert deferred["retained"]["from_seq"] == 1
    assert deferred["retained"]["blockers"][0]["job_id"] == job["job_id"]
    assert client.get("/api/changes/prune").json()["deferred"] is True

    # Once the job goes away, the same call prunes normally.
    fail_job(
        job["job_id"],
        claimed["worker_token"],
        state="interrupted",
        error="test cleanup",
        generation_id=job["generation_id"],
    )
    pruned = client.post("/api/changes/prune", json={"upto_seq": 999_999}).json()
    assert pruned["deferred"] is False
    assert pruned["deleted_count"] == 1
    assert pruned["remaining_count"] == 0
    assert pruned["latest_seq"] == 1


def test_prune_during_rebuild_keeps_changes_needed_for_replay(client: TestClient):
    for index in range(16):
        assert client.put(
            f"/api/pages/book-{index:02d}",
            json={
                "title": f"Archive page {index:02d}",
                "body": f"Le café numéro {index:02d} et le señor.",
            },
        ).status_code == 200

    trigger = client.post("/api/index/rebuild", json={"accent_policy": "fold"})
    assert trigger.status_code == 202
    job_id = trigger.json()["job"]["job_id"]
    running = _wait_for_base_seq(client)
    base_seq = running["base_seq"]
    assert base_seq == 16

    # Correction and deletion committed while the rebuild scans/catches up.
    corrected = client.put(
        "/api/pages/book-03",
        json={
            "title": "Archive page 03 — corrigé",
            "body": "Le café fermé; le señor relit le résumé corrigé.",
        },
    )
    assert corrected.status_code == 200
    destroyed = client.delete("/api/pages/book-04")
    assert destroyed.status_code == 200
    correction_seq = corrected.json()["revision"]
    delete_seq = destroyed.json()["revision"]
    assert correction_seq == base_seq + 1
    assert delete_seq == base_seq + 2

    # Pruning mid-rebuild reclaims only the snapshot-safe prefix.  The
    # correction and the delete stay in the log for the catch-up to replay.
    pruned = client.post(
        "/api/changes/prune", json={"upto_seq": 10_000_000}
    ).json()
    assert pruned["deferred"] is False
    assert pruned["deleted_count"] == base_seq
    assert pruned["deleted_upto_seq"] == base_seq
    assert pruned["retained"]["from_seq"] == base_seq + 1
    assert pruned["remaining_count"] == 2
    assert pruned["remaining_from_seq"] == correction_seq
    assert pruned["latest_seq"] == delete_seq

    status = _wait_for_terminal(client, job_id)
    assert status["latest_rebuild_job"]["state"] == "succeeded"
    assert status["active_policy"] == "fold"

    # The replay was not harmed: the correction is searchable under fold and
    # the deleted page is neither searchable nor readable.
    folded = _search(client, "cafe")
    ids = [hit["page_id"] for hit in folded["hits"]]
    assert "book-03" in ids
    assert "book-04" not in ids
    assert len(ids) == 15
    hit3 = [hit for hit in folded["hits"] if hit["page_id"] == "book-03"][0]
    assert hit3["body"] == "Le café fermé; le señor relit le résumé corrigé."
    assert hit3["revision"] == correction_seq
    assert _search(client, "corrige")["total"] == 1
    assert client.get("/api/pages/book-04").status_code == 404

    # After the switch nothing depends on the log: the rest is prunable, and
    # the committed sequence/revisions still do not regress.
    final = client.post("/api/changes/prune", json={"upto_seq": 10_000_000}).json()
    assert final["deleted_count"] == 2
    assert final["remaining_count"] == 0
    assert final["latest_seq"] == delete_seq
    page = client.get("/api/pages/book-03").json()
    assert page["revision"] == correction_seq


def test_concurrent_prune_never_double_counts(tmp_path: Path):
    configure_database(tmp_path / "concurrent.db")
    from app.db import init_db
    from app.archive import upsert_page
    from app.prune import prune_changes

    init_db()
    total = 40
    for index in range(total):
        upsert_page(f"c-{index:02d}", f"titre {index}", f"corps café {index}")

    results: list[dict] = []
    errors: list[BaseException] = []

    def run_prune() -> None:
        try:
            results.append(
                prune_changes(upto_seq=10_000_000, batch_size=7)
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=run_prune) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors
    assert len(results) == 2
    assert all(result["completed"] for result in results)
    # Exactly `total` rows were deleted across both pruners: no double count.
    assert sum(result["deleted_count"] for result in results) == total
    assert max(result["deleted_upto_seq"] for result in results) == total
    assert all(result["remaining_count"] == 0 for result in results)
    assert all(result["latest_seq"] == total for result in results)


def test_prune_batch_limit_pauses_and_retry_continues(client: TestClient):
    for index in range(25):
        assert client.put(
            f"/api/pages/b-{index:02d}",
            json={"title": f"t{index}", "body": f"corps café {index}"},
        ).status_code == 200

    first = client.post(
        "/api/changes/prune",
        json={"upto_seq": 25, "batch_size": 5, "max_batches": 2},
    ).json()
    assert first["deleted_count"] == 10
    assert first["batches_completed"] == 2
    assert first["completed"] is False
    assert first["remaining_count"] == 15

    # A retry of the same call continues where the interrupted one stopped.
    second = client.post(
        "/api/changes/prune",
        json={"upto_seq": 25, "batch_size": 5, "max_batches": 10},
    ).json()
    assert second["deleted_count"] == 15
    assert second["completed"] is True
    assert second["remaining_count"] == 0
    assert second["latest_seq"] == 25
    assert second["deleted_upto_seq"] == 25


def test_restart_and_rebuild_after_full_prune(tmp_path: Path):
    configure_database(tmp_path / "restart-prune.db")
    from app.archive import get_page, search, upsert_page
    from app.db import claim_rebuild, connect, get_job, get_status, init_db
    from app.prune import prune_changes
    from app.rebuild import start_rebuild_thread

    init_db()
    first_rev = upsert_page("p1", "Café", "Le café et le señor.")["revision"]
    second_rev = upsert_page("p2", "Naïve", "Un naïve résumé.")["revision"]

    pruned = prune_changes(upto_seq=10_000_000)
    assert pruned["deleted_count"] == 2
    assert pruned["latest_seq"] == second_rev

    # sqlite_sequence is never reset by pruning.
    conn = connect()
    try:
        persisted = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'changes'"
        ).fetchone()
        assert persisted is not None and int(persisted[0]) == second_rev
    finally:
        conn.close()

    # Restart: schema init and the active index survive the emptied log.
    init_db()
    status = get_status()
    assert status["active_policy"] == "keep"
    assert search("café", limit=10, offset=0)["total"] == 1

    # Revisions keep climbing after the restart; nothing is reused.
    third_rev = upsert_page("p1", "Café révisé", "Le café corrigé et le señor.")["revision"]
    assert third_rev > second_rev > first_rev

    # A fresh rebuild after the prune works and sees the latest committed state.
    claimed = claim_rebuild("fold")
    assert claimed["created"] is True
    job_id = claimed["job"]["job_id"]
    start_rebuild_thread(
        job_id=job_id,
        generation_id=claimed["job"]["generation_id"],
        policy="fold",
        claim_token=claimed["worker_token"],
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        job = get_job(job_id)
        if job["state"] in {"succeeded", "failed", "interrupted"}:
            break
        time.sleep(0.03)
    else:
        raise AssertionError("rebuild after prune did not finish")
    assert job["state"] == "succeeded", job
    assert get_status()["active_policy"] == "fold"

    hits = search("cafe", limit=10, offset=0)["hits"]
    assert [hit["page_id"] for hit in hits] == ["p1"]
    assert hits[0]["body"] == "Le café corrigé et le señor."
    assert hits[0]["revision"] == third_rev
    assert get_page("p2") is not None  # untouched by pruning

    # The log accumulated only the post-prune write; it is prunable again.
    rep_run = prune_changes(upto_seq=10_000_000)
    assert rep_run["deleted_count"] == 1
    assert rep_run["latest_seq"] == third_rev
