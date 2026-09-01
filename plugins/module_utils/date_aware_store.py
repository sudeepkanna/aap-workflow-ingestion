from __future__ import annotations

import hashlib
from datetime import date
from typing import Any, Dict, Optional

from .postgres_store import PostgresStore


class DateAwarePostgresStore(PostgresStore):
    def ensure_schema(self) -> None:
        super().ensure_schema()
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.ingestion_state_by_date (
            workflow_template_id BIGINT NOT NULL,
            run_date DATE NOT NULL,
            last_root_workflow_job_id BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (workflow_template_id, run_date)
        );
        ALTER TABLE {self.schema}.execution_nodes
            ADD COLUMN IF NOT EXISTS unified_job_raw JSONB;
        """
        with self._conn().cursor() as cur:
            cur.execute(sql)
        self._conn().commit()

    @staticmethod
    def _date_lock_key(template_id: int, run_date: date) -> int:
        digest = hashlib.blake2b(
            f"{template_id}:{run_date.isoformat()}".encode("utf-8"),
            digest_size=8,
        ).digest()
        return int.from_bytes(digest, byteorder="big", signed=True)

    def acquire_date_lock(self, template_id: int, run_date: date) -> bool:
        with self._conn().cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (self._date_lock_key(template_id, run_date),),
            )
            return bool(cur.fetchone()[0])

    def release_date_lock(self, template_id: int, run_date: date) -> None:
        with self._conn().cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_unlock(%s)",
                (self._date_lock_key(template_id, run_date),),
            )
        self._conn().commit()

    def checkpoint_for_date(self, template_id: int, run_date: date) -> int:
        if not self.schema_exists():
            return 0
        with self._conn().cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f"{self.schema}.ingestion_state_by_date",))
            if cur.fetchone()[0] is None:
                return 0
            cur.execute(
                f"SELECT last_root_workflow_job_id "
                f"FROM {self.schema}.ingestion_state_by_date "
                "WHERE workflow_template_id=%s AND run_date=%s",
                (template_id, run_date),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def update_checkpoint_for_date(self, template_id: int, run_date: date, root_id: int) -> None:
        sql = f"""
        INSERT INTO {self.schema}.ingestion_state_by_date
        (workflow_template_id, run_date, last_root_workflow_job_id)
        VALUES (%s,%s,%s)
        ON CONFLICT (workflow_template_id, run_date) DO UPDATE SET
          last_root_workflow_job_id=GREATEST(
            {self.schema}.ingestion_state_by_date.last_root_workflow_job_id,
            EXCLUDED.last_root_workflow_job_id
          ),
          updated_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, (template_id, run_date, root_id))

    def upsert_node(
        self,
        root_id: int,
        parent_workflow_id: int,
        depth: int,
        node: Dict[str, Any],
        unified: Optional[Dict[str, Any]],
    ) -> None:
        values = (
            node["id"],
            root_id,
            parent_workflow_id,
            depth,
            node.get("identifier"),
            (unified or {}).get("id"),
            (unified or {}).get("type"),
            bool(node.get("do_not_run", False)),
            self._json(unified or {}),
            self._json(node),
        )
        sql = f"""
        INSERT INTO {self.schema}.execution_nodes
        (node_id, root_workflow_job_id, parent_workflow_job_id, depth, identifier,
         unified_job_id, unified_job_type, do_not_run, unified_job_raw, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (node_id) DO UPDATE SET
          unified_job_id=EXCLUDED.unified_job_id,
          unified_job_type=EXCLUDED.unified_job_type,
          do_not_run=EXCLUDED.do_not_run,
          unified_job_raw=EXCLUDED.unified_job_raw,
          raw=EXCLUDED.raw,
          ingested_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, values)
