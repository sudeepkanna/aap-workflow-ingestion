from __future__ import annotations

from datetime import date

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
        )
        """
        with self._conn().cursor() as cur:
            cur.execute(sql)
        self._conn().commit()

    def checkpoint_for_date(self, template_id: int, run_date: date) -> int:
        if not self.schema_exists():
            return 0
        with self._conn().cursor() as cur:
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
