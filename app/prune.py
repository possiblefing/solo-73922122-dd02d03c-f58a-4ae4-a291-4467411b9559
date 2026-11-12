"""Batched, rebuild-aware pruning of the OCR change log.

The ``changes`` table is the durable replay log used by background index
rebuilds.  Long-running OCR correction workloads leave superseded duplicate
bodies in it.  This module deletes the safe prefix of the log in small,
individually committed batches.

Safety contract (coordinated with rebuild claim, snapshot registration,
catch-up and the atomic switch):

* A rebuild that has registered its snapshot base sequence ``B`` has already
  copied every page state committed up to ``B`` into its shadow index and only
  replays rows with ``seq > B``.  Rows ``<= B`` are therefore safe to delete
  for that rebuild.  The pruning floor is the minimum registered ``base_seq``
  over all active rebuild jobs.
* A rebuild that has been claimed but has not registered its snapshot base
  yet (``base_seq = 0``) makes the safe boundary unknowable, so pruning is
  *deferred*: nothing is deleted and the caller can retry later.
* When no rebuild is active, every committed row is safe to delete.
* Each batch is its own short ``BEGIN IMMEDIATE`` transaction, so page
  corrections and deletes keep committing while a prune runs; no lock is held
  for the whole operation.
* ``sqlite_sequence`` is never touched.  After the log has been emptied the
  next change still receives a higher sequence than any ever assigned, so
  page revisions never regress or get reused, and a later rebuild snapshot
  never mistakes a recycled sequence for a replayable row.

Only log rows are deleted.  No ``VACUUM``/file shrinking and no FTS/generation
cleanup is performed here.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any

from .config import SETTINGS
from .db import ACTIVE_JOB_STATES, begin_immediate, connect, utc_now

MAX_BATCH_SIZE = 10_000


def _latest_committed_seq(conn: sqlite3.Connection) -> int:
    """The last sequence ever assigned to the change log.

    Read from ``sqlite_sequence`` so it survives a full prune of the table;
    falls back to ``MAX(seq)`` before the first insert.
    """

    row = conn.execute(
        "SELECT seq FROM sqlite_sequence WHERE name = 'changes'"
    ).fetchone()
    if row is not None:
        return int(row[0])
    return int(conn.execute("SELECT COALESCE(MAX(seq), 0) FROM changes").fetchone()[0])


def _active_blockers(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Active rebuild jobs and the snapshot base each one still replays from."""

    placeholders = ", ".join("?" for _ in ACTIVE_JOB_STATES)
    rows = conn.execute(
        f"""
        SELECT id, state, base_seq, applied_seq
          FROM rebuild_jobs
         WHERE state IN ({placeholders})
         ORDER BY created_epoch
        """,
        ACTIVE_JOB_STATES,
    ).fetchall()
    return [
        {
            "job_id": row["id"],
            "state": row["state"],
            "base_seq": int(row["base_seq"]),
            "applied_seq": int(row["applied_seq"]),
        }
        for row in rows
    ]


def _decision(conn: sqlite3.Connection, upto_seq: int | None) -> dict[str, Any]:
    """Compute the safe deletion boundary from one consistent snapshot."""

    blockers = _active_blockers(conn)
    current_max = int(
        conn.execute("SELECT COALESCE(MAX(seq), 0) FROM changes").fetchone()[0]
    )
    latest_seq = _latest_committed_seq(conn)
    total_count = int(conn.execute("SELECT COUNT(*) FROM changes").fetchone()[0])

    unregistered = [b for b in blockers if b["base_seq"] <= 0]
    deferred = bool(unregistered)
    if deferred:
        floor = 0
    elif blockers:
        floor = min(b["base_seq"] for b in blockers)
    else:
        # No rebuild is replaying the log: everything committed is safe.
        floor = current_max

    if upto_seq is None:
        target = floor
    else:
        target = min(upto_seq, floor)
    target = max(0, min(target, current_max))

    if deferred:
        job_list = ", ".join(
            f"{b['job_id']} ({b['state']})" for b in unregistered
        )
        reason = (
            f"rebuild job(s) {job_list} have not registered a snapshot base "
            "sequence yet; pruning is deferred until the snapshot start is known"
        )
        retained_from = 1 if total_count else None
    elif blockers:
        job_list = ", ".join(
            f"{b['job_id']} ({b['state']}, snapshot base {b['base_seq']})"
            for b in blockers
        )
        reason = (
            f"active rebuild job(s) {job_list} still replay changes with "
            f"seq > {floor}; sequences above {floor} must be retained"
        )
        retained_from = floor + 1
    else:
        reason = "no active rebuild depends on the change log"
        retained_from = None

    return {
        "requested_upto_seq": upto_seq,
        "effective_upto_seq": target,
        "deferred": deferred,
        "retained": {
            "from_seq": retained_from,
            "reason": reason,
            "blockers": blockers,
        },
        "latest_seq": latest_seq,
        "current_max_seq": current_max,
        "total_count": total_count,
    }


def prune_preview(upto_seq: int | None = None) -> dict[str, Any]:
    """Read-only view of the safe pruning boundary; deletes nothing."""

    if upto_seq is not None and upto_seq < 0:
        raise ValueError("upto_seq must be >= 0")
    conn = connect(query_only=True)
    try:
        conn.execute("BEGIN")
        decision = _decision(conn, upto_seq)
        deletable = int(
            conn.execute(
                "SELECT COUNT(*) FROM changes WHERE seq <= ?",
                (decision["effective_upto_seq"],),
            ).fetchone()[0]
        )
        first_seq = conn.execute("SELECT MIN(seq) FROM changes").fetchone()[0]
        conn.execute("COMMIT")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    decision.update(
        {
            "deletable_count": deletable,
            "first_seq": int(first_seq) if first_seq is not None else None,
            "default_batch_size": SETTINGS.prune_batch_size,
            "inspected_at": utc_now(),
        }
    )
    return decision


def _delete_one_batch(target: int, batch_size: int) -> tuple[int, int]:
    """Delete one committed batch; returns (rows_deleted, max_seq_deleted)."""

    conn = connect()
    try:
        begin_immediate(conn)
        row = conn.execute(
            """
            SELECT COALESCE(MAX(seq), 0)
              FROM (SELECT seq FROM changes WHERE seq <= ? ORDER BY seq LIMIT ?)
            """,
            (target, batch_size),
        ).fetchone()
        batch_max = int(row[0])
        if batch_max == 0:
            conn.commit()
            return 0, 0
        cur = conn.execute(
            """
            DELETE FROM changes
             WHERE seq IN (
                 SELECT seq FROM changes WHERE seq <= ? ORDER BY seq LIMIT ?
             )
            """,
            (target, batch_size),
        )
        count = int(cur.rowcount)
        conn.commit()
        return count, batch_max
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _remaining_upto(target: int) -> int:
    conn = connect(query_only=True)
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM changes WHERE seq <= ?", (target,)
            ).fetchone()[0]
        )
    finally:
        conn.close()


def prune_changes(
    *,
    upto_seq: int | None = None,
    batch_size: int | None = None,
    max_batches: int | None = None,
) -> dict[str, Any]:
    """Delete the safe prefix of the change log in committed batches.

    The deletion target is ``min(upto_seq, safety floor, current max seq)``.
    Batches commit individually: an interruption keeps completed batches and a
    retry of the same call continues where the previous one stopped.  When two
    prunes run concurrently their batch transactions serialise, so every row
    is deleted (and counted) exactly once.
    """

    if upto_seq is not None and upto_seq < 0:
        raise ValueError("upto_seq must be >= 0")
    if batch_size is not None and batch_size < 1:
        raise ValueError("batch_size must be >= 1")
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be >= 1")
    batch = min(batch_size or SETTINGS.prune_batch_size, MAX_BATCH_SIZE)

    started = time.monotonic()

    # Decide the safe boundary in one short transaction.  The result stays
    # valid for the whole run: rows <= target were committed before this
    # point, so any rebuild claimed later snapshots them as page state, and
    # every already-active rebuild has a registered base >= target.
    conn = connect()
    try:
        begin_immediate(conn)
        decision = _decision(conn, upto_seq)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    target = decision["effective_upto_seq"]
    deleted = 0
    batches = 0
    deleted_upto = 0
    completed = not decision["deferred"]

    if not decision["deferred"] and target > 0:
        while True:
            if max_batches is not None and batches >= max_batches:
                # Bounded call: report honestly whether the target is drained.
                completed = _remaining_upto(target) == 0
                break
            count, batch_max = _delete_one_batch(target, batch)
            if count == 0:
                break
            deleted += count
            batches += 1
            deleted_upto = batch_max

    conn = connect(query_only=True)
    try:
        latest_seq = _latest_committed_seq(conn)
        remaining = int(
            conn.execute("SELECT COUNT(*) FROM changes").fetchone()[0]
        )
        remaining_from = conn.execute("SELECT MIN(seq) FROM changes").fetchone()[0]
    finally:
        conn.close()

    return {
        **decision,
        "completed": completed,
        "deleted_count": deleted,
        "deleted_upto_seq": deleted_upto,
        "batches_completed": batches,
        "batch_size": batch,
        "max_batches": max_batches,
        "latest_seq": latest_seq,
        "remaining_count": remaining,
        "remaining_from_seq": int(remaining_from) if remaining_from is not None else None,
        "duration_ms": round((time.monotonic() - started) * 1000.0, 1),
        "vacuum_performed": False,
        "finished_at": utc_now(),
    }
