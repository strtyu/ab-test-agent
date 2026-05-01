from __future__ import annotations

import json
from datetime import datetime
from typing import List, Optional

from ab_agent.db.database import get_connection


def _fetch_one(conn, sql: str, params: tuple = ()) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return dict(row) if row else None


def _fetch_all(conn, sql: str, params: tuple = ()) -> List[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def _execute(conn, sql: str, params: tuple = ()) -> None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()


class TestRepo:
    def create(self, test_id: str, test_name: str, config_json: str, chat_history_json: str = "") -> None:
        conn = get_connection()
        _execute(conn,
            "INSERT INTO tests (id, test_name, config_json, status, created_at, chat_history_json) VALUES (%s,%s,%s,'running',%s,%s)",
            (test_id, test_name, config_json, datetime.utcnow().isoformat(), chat_history_json or ""),
        )

    def list_all(self) -> List[dict]:
        return _fetch_all(get_connection(), "SELECT * FROM tests ORDER BY created_at DESC")

    def get(self, test_id: str) -> Optional[dict]:
        return _fetch_one(get_connection(), "SELECT * FROM tests WHERE id=%s", (test_id,))

    def mark_ended(self, test_id: str) -> None:
        conn = get_connection()
        _execute(conn,
            "UPDATE tests SET status='ended', ended_at=%s WHERE id=%s",
            (datetime.utcnow().isoformat(), test_id),
        )

    def update_config(self, test_id: str, config_json: str) -> None:
        _execute(get_connection(), "UPDATE tests SET config_json=%s WHERE id=%s", (config_json, test_id))

    def update_name(self, test_id: str, test_name: str) -> None:
        _execute(get_connection(), "UPDATE tests SET test_name=%s WHERE id=%s", (test_name, test_id))

    def update_chat_history(self, test_id: str, chat_history_json: str) -> None:
        _execute(get_connection(), "UPDATE tests SET chat_history_json=%s WHERE id=%s", (chat_history_json, test_id))

    def delete(self, test_id: str) -> None:
        conn = get_connection()
        _execute(conn, "DELETE FROM analyses WHERE test_id=%s", (test_id,))
        _execute(conn, "DELETE FROM snapshots WHERE test_id=%s", (test_id,))
        _execute(conn, "DELETE FROM tests WHERE id=%s", (test_id,))


class SnapshotRepo:
    def save(
        self,
        snapshot_id: str,
        test_id: str,
        ctrl_metrics: dict,
        test_metrics: dict,
        slices: dict,
        screenshot_path: str = "",
        dashboard_path: str = "",
        dashboard_html: str = "",
    ) -> None:
        slices_serialized = json.dumps(
            slices if isinstance(slices, dict) and all(
                isinstance(v, dict) and "ctrl" in v for v in slices.values()
            ) else {
                k: {"slice_key": v.slice_key, "control_metrics": v.control_metrics, "test_metrics": v.test_metrics}
                for k, v in slices.items()
            }
        )
        conn = get_connection()
        _execute(conn,
            """INSERT INTO snapshots
               (id, test_id, created_at, ctrl_metrics_json, test_metrics_json, slices_json,
                screenshot_path, dashboard_path, dashboard_html)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                snapshot_id, test_id, datetime.utcnow().isoformat(),
                json.dumps(ctrl_metrics), json.dumps(test_metrics), slices_serialized,
                screenshot_path, dashboard_path, dashboard_html,
            ),
        )

    def latest(self, test_id: str) -> Optional[dict]:
        return _fetch_one(get_connection(),
            "SELECT * FROM snapshots WHERE test_id=%s ORDER BY created_at DESC LIMIT 1",
            (test_id,),
        )

    def list_for_test(self, test_id: str) -> List[dict]:
        return _fetch_all(get_connection(),
            "SELECT * FROM snapshots WHERE test_id=%s ORDER BY created_at DESC", (test_id,),
        )


class AnalysisRepo:
    def save(
        self,
        analysis_id: str,
        test_id: str,
        results_json: str,
        recommendation: str,
        narrative: str,
        screenshot_path: str = "",
    ) -> None:
        conn = get_connection()
        _execute(conn,
            """INSERT INTO analyses
               (id, test_id, created_at, results_json, recommendation, narrative, screenshot_path)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (analysis_id, test_id, datetime.utcnow().isoformat(),
             results_json, recommendation, narrative, screenshot_path),
        )

    def get(self, analysis_id: str) -> Optional[dict]:
        return _fetch_one(get_connection(), "SELECT * FROM analyses WHERE id=%s", (analysis_id,))

    def list_for_test(self, test_id: str) -> List[dict]:
        return _fetch_all(get_connection(),
            "SELECT * FROM analyses WHERE test_id=%s ORDER BY created_at DESC", (test_id,),
        )
