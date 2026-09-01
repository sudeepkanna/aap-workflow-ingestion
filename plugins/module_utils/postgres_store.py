from __future__ import annotations

import json
import re
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, Optional

import psycopg


class StoreError(RuntimeError):
    pass


class PostgresStore:
    def __init__(self, dsn: str, schema: str = "aap_ingest") -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
            raise StoreError("Invalid PostgreSQL schema name")
        self.dsn = dsn
        self.schema = schema
        self.connection: Optional[psycopg.Connection] = None

    def connect(self) -> None:
        try:
            self.connection = psycopg.connect(self.dsn, autocommit=False)
        except psycopg.Error as exc:
            raise StoreError(f"Unable to connect to PostgreSQL: {exc}") from exc

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None

    def _conn(self) -> psycopg.Connection:
        if not self.connection:
            raise StoreError("PostgreSQL connection is not open")
        return self.connection

    def acquire_lock(self, template_id: int) -> bool:
        with self._conn().cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (template_id,))
            return bool(cur.fetchone()[0])

    def release_lock(self, template_id: int) -> None:
        with self._conn().cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (template_id,))
        self._conn().commit()

    def schema_exists(self) -> bool:
        with self._conn().cursor() as cur:
            cur.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name=%s)",
                (self.schema,),
            )
            return bool(cur.fetchone()[0])

    def ensure_schema(self) -> None:
        s = self.schema
        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {s};

        CREATE TABLE IF NOT EXISTS {s}.ingestion_state (
            workflow_template_id BIGINT PRIMARY KEY,
            last_root_workflow_job_id BIGINT NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.workflow_runs (
            workflow_job_id BIGINT PRIMARY KEY,
            root_workflow_job_id BIGINT NOT NULL,
            workflow_template_id BIGINT,
            parent_workflow_job_id BIGINT,
            depth INTEGER NOT NULL,
            name TEXT,
            status TEXT,
            launch_type TEXT,
            started TIMESTAMPTZ,
            finished TIMESTAMPTZ,
            elapsed DOUBLE PRECISION,
            organization_name TEXT,
            inventory_id BIGINT,
            inventory_name TEXT,
            extra_vars JSONB,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.execution_nodes (
            node_id BIGINT PRIMARY KEY,
            root_workflow_job_id BIGINT NOT NULL,
            parent_workflow_job_id BIGINT NOT NULL,
            depth INTEGER NOT NULL,
            identifier TEXT,
            unified_job_id BIGINT,
            unified_job_type TEXT,
            do_not_run BOOLEAN,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.jobs (
            job_id BIGINT PRIMARY KEY,
            root_workflow_job_id BIGINT NOT NULL,
            parent_workflow_job_id BIGINT NOT NULL,
            node_id BIGINT,
            depth INTEGER NOT NULL,
            name TEXT,
            status TEXT,
            launch_type TEXT,
            started TIMESTAMPTZ,
            finished TIMESTAMPTZ,
            elapsed DOUBLE PRECISION,
            job_template_id BIGINT,
            job_template_name TEXT,
            inventory_id BIGINT,
            inventory_name TEXT,
            execution_environment_name TEXT,
            job_slice_number INTEGER,
            job_slice_count INTEGER,
            limit_pattern TEXT,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.inventories (
            inventory_id BIGINT PRIMARY KEY,
            name TEXT,
            organization_name TEXT,
            kind TEXT,
            variables JSONB,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.job_host_summaries (
            summary_id BIGINT PRIMARY KEY,
            root_workflow_job_id BIGINT NOT NULL,
            job_id BIGINT NOT NULL,
            host_id BIGINT,
            host_name TEXT,
            changed BIGINT,
            failures BIGINT,
            ok BIGINT,
            skipped BIGINT,
            dark BIGINT,
            processed BIGINT,
            failed BOOLEAN,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.job_events (
            event_id BIGINT PRIMARY KEY,
            root_workflow_job_id BIGINT NOT NULL,
            job_id BIGINT NOT NULL,
            counter BIGINT,
            event TEXT,
            event_display TEXT,
            event_level INTEGER,
            created TIMESTAMPTZ,
            host_name TEXT,
            play_name TEXT,
            task_name TEXT,
            task_action TEXT,
            changed BOOLEAN,
            failed BOOLEAN,
            unreachable BOOLEAN,
            result JSONB,
            raw JSONB NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {s}.workflow_metrics (
            root_workflow_job_id BIGINT PRIMARY KEY,
            workflow_count BIGINT NOT NULL,
            job_count BIGINT NOT NULL,
            successful_jobs BIGINT NOT NULL,
            failed_jobs BIGINT NOT NULL,
            host_summary_count BIGINT NOT NULL,
            event_count BIGINT NOT NULL,
            changed_events BIGINT NOT NULL,
            failed_events BIGINT NOT NULL,
            unreachable_events BIGINT NOT NULL,
            collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_workflow_runs_root ON {s}.workflow_runs(root_workflow_job_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_root ON {s}.jobs(root_workflow_job_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_parent ON {s}.jobs(parent_workflow_job_id);
        CREATE INDEX IF NOT EXISTS idx_events_job ON {s}.job_events(job_id);
        CREATE INDEX IF NOT EXISTS idx_events_root ON {s}.job_events(root_workflow_job_id);
        CREATE INDEX IF NOT EXISTS idx_events_host ON {s}.job_events(host_name);
        CREATE INDEX IF NOT EXISTS idx_events_task ON {s}.job_events(task_name);
        CREATE INDEX IF NOT EXISTS idx_hosts_job ON {s}.job_host_summaries(job_id);
        """
        try:
            with self._conn().cursor() as cur:
                cur.execute(ddl)
            self._conn().commit()
        except psycopg.Error as exc:
            self._conn().rollback()
            raise StoreError(f"Unable to initialize schema: {exc}") from exc

    def checkpoint(self, template_id: int) -> int:
        if not self.schema_exists():
            return 0
        with self._conn().cursor() as cur:
            cur.execute(
                f"SELECT last_root_workflow_job_id FROM {self.schema}.ingestion_state WHERE workflow_template_id=%s",
                (template_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0

    @contextmanager
    def root_transaction(self) -> Iterator[None]:
        try:
            yield
            self._conn().commit()
        except Exception:
            self._conn().rollback()
            raise

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value if value is not None else {})

    def upsert_workflow(self, root_id: int, parent_id: Optional[int], depth: int, item: Dict[str, Any]) -> None:
        sf = item.get("summary_fields", {})
        inv = sf.get("inventory", {}) or {}
        org = sf.get("organization", {}) or {}
        template = sf.get("workflow_job_template", {}) or {}
        values = (
            item["id"], root_id, template.get("id") or item.get("workflow_job_template"), parent_id, depth,
            item.get("name"), item.get("status"), item.get("launch_type"), item.get("started"),
            item.get("finished"), item.get("elapsed"), org.get("name"), inv.get("id"), inv.get("name"),
            self._json(item.get("extra_vars") or {}), self._json(item),
        )
        sql = f"""
        INSERT INTO {self.schema}.workflow_runs
        (workflow_job_id, root_workflow_job_id, workflow_template_id, parent_workflow_job_id, depth,
         name, status, launch_type, started, finished, elapsed, organization_name, inventory_id,
         inventory_name, extra_vars, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (workflow_job_id) DO UPDATE SET
          status=EXCLUDED.status, started=EXCLUDED.started, finished=EXCLUDED.finished,
          elapsed=EXCLUDED.elapsed, raw=EXCLUDED.raw, ingested_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, values)

    def upsert_node(self, root_id: int, parent_workflow_id: int, depth: int, node: Dict[str, Any], unified: Optional[Dict[str, Any]]) -> None:
        values = (
            node["id"], root_id, parent_workflow_id, depth, node.get("identifier"),
            (unified or {}).get("id"), (unified or {}).get("type"), bool(node.get("do_not_run", False)),
            self._json(node),
        )
        sql = f"""
        INSERT INTO {self.schema}.execution_nodes
        (node_id, root_workflow_job_id, parent_workflow_job_id, depth, identifier, unified_job_id,
         unified_job_type, do_not_run, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (node_id) DO UPDATE SET unified_job_id=EXCLUDED.unified_job_id,
          unified_job_type=EXCLUDED.unified_job_type, do_not_run=EXCLUDED.do_not_run,
          raw=EXCLUDED.raw, ingested_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, values)

    def upsert_job(self, root_id: int, parent_workflow_id: int, node_id: int, depth: int, job: Dict[str, Any]) -> None:
        sf = job.get("summary_fields", {})
        jt = sf.get("job_template", {}) or {}
        inv = sf.get("inventory", {}) or {}
        ee = sf.get("execution_environment", {}) or {}
        values = (
            job["id"], root_id, parent_workflow_id, node_id, depth, job.get("name"), job.get("status"),
            job.get("launch_type"), job.get("started"), job.get("finished"), job.get("elapsed"),
            jt.get("id") or job.get("job_template"), jt.get("name"), inv.get("id") or job.get("inventory"),
            inv.get("name"), ee.get("name"), job.get("job_slice_number"), job.get("job_slice_count"),
            job.get("limit"), self._json(job),
        )
        sql = f"""
        INSERT INTO {self.schema}.jobs
        (job_id, root_workflow_job_id, parent_workflow_job_id, node_id, depth, name, status, launch_type,
         started, finished, elapsed, job_template_id, job_template_name, inventory_id, inventory_name,
         execution_environment_name, job_slice_number, job_slice_count, limit_pattern, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (job_id) DO UPDATE SET status=EXCLUDED.status, started=EXCLUDED.started,
          finished=EXCLUDED.finished, elapsed=EXCLUDED.elapsed, raw=EXCLUDED.raw, ingested_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, values)

    def upsert_inventory(self, inventory: Dict[str, Any]) -> None:
        sf = inventory.get("summary_fields", {})
        org = sf.get("organization", {}) or {}
        values = (
            inventory["id"], inventory.get("name"), org.get("name"), inventory.get("kind"),
            self._json(inventory.get("variables") or {}), self._json(inventory),
        )
        sql = f"""
        INSERT INTO {self.schema}.inventories
        (inventory_id, name, organization_name, kind, variables, raw)
        VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (inventory_id) DO UPDATE SET name=EXCLUDED.name, organization_name=EXCLUDED.organization_name,
          kind=EXCLUDED.kind, variables=EXCLUDED.variables, raw=EXCLUDED.raw, ingested_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, values)

    def upsert_host_summaries(self, root_id: int, job_id: int, rows: Iterable[Dict[str, Any]]) -> int:
        sql = f"""
        INSERT INTO {self.schema}.job_host_summaries
        (summary_id, root_workflow_job_id, job_id, host_id, host_name, changed, failures, ok,
         skipped, dark, processed, failed, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (summary_id) DO UPDATE SET changed=EXCLUDED.changed, failures=EXCLUDED.failures,
          ok=EXCLUDED.ok, skipped=EXCLUDED.skipped, dark=EXCLUDED.dark, processed=EXCLUDED.processed,
          failed=EXCLUDED.failed, raw=EXCLUDED.raw, ingested_at=NOW()
        """
        count = 0
        with self._conn().cursor() as cur:
            for item in rows:
                host = item.get("summary_fields", {}).get("host", {}) or {}
                cur.execute(sql, (
                    item["id"], root_id, job_id, host.get("id") or item.get("host"), host.get("name"),
                    item.get("changed", 0), item.get("failures", 0), item.get("ok", 0),
                    item.get("skipped", 0), item.get("dark", 0), item.get("processed", 0),
                    bool(item.get("failed", False)), self._json(item),
                ))
                count += 1
        return count

    def upsert_events(self, root_id: int, job_id: int, rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        sql = f"""
        INSERT INTO {self.schema}.job_events
        (event_id, root_workflow_job_id, job_id, counter, event, event_display, event_level, created,
         host_name, play_name, task_name, task_action, changed, failed, unreachable, result, raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
        ON CONFLICT (event_id) DO UPDATE SET event=EXCLUDED.event, event_display=EXCLUDED.event_display,
          event_level=EXCLUDED.event_level, result=EXCLUDED.result, raw=EXCLUDED.raw, ingested_at=NOW()
        """
        metrics = {"events": 0, "changed": 0, "failed": 0, "unreachable": 0}
        with self._conn().cursor() as cur:
            for item in rows:
                data = item.get("event_data", {}) or {}
                result = data.get("res", {}) or {}
                changed = bool(result.get("changed", False))
                failed = bool(result.get("failed", False) or item.get("event") == "runner_on_failed")
                unreachable = bool(result.get("unreachable", False) or item.get("event") == "runner_on_unreachable")
                cur.execute(sql, (
                    item["id"], root_id, job_id, item.get("counter"), item.get("event"),
                    item.get("event_display"), item.get("event_level"), item.get("created"), data.get("host"),
                    data.get("play"), data.get("task"), data.get("task_action"), changed, failed, unreachable,
                    self._json(result), self._json(item),
                ))
                metrics["events"] += 1
                metrics["changed"] += int(changed)
                metrics["failed"] += int(failed)
                metrics["unreachable"] += int(unreachable)
        return metrics

    def upsert_metrics(self, root_id: int, metrics: Dict[str, int]) -> None:
        sql = f"""
        INSERT INTO {self.schema}.workflow_metrics
        (root_workflow_job_id, workflow_count, job_count, successful_jobs, failed_jobs,
         host_summary_count, event_count, changed_events, failed_events, unreachable_events)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (root_workflow_job_id) DO UPDATE SET workflow_count=EXCLUDED.workflow_count,
          job_count=EXCLUDED.job_count, successful_jobs=EXCLUDED.successful_jobs,
          failed_jobs=EXCLUDED.failed_jobs, host_summary_count=EXCLUDED.host_summary_count,
          event_count=EXCLUDED.event_count, changed_events=EXCLUDED.changed_events,
          failed_events=EXCLUDED.failed_events, unreachable_events=EXCLUDED.unreachable_events,
          collected_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, (
                root_id, metrics["workflows"], metrics["jobs"], metrics["successful_jobs"],
                metrics["failed_jobs"], metrics["hosts"], metrics["events"], metrics["changed"],
                metrics["failed_events"], metrics["unreachable"],
            ))

    def update_checkpoint(self, template_id: int, root_id: int) -> None:
        sql = f"""
        INSERT INTO {self.schema}.ingestion_state(workflow_template_id, last_root_workflow_job_id)
        VALUES (%s,%s)
        ON CONFLICT (workflow_template_id) DO UPDATE SET
          last_root_workflow_job_id=GREATEST({self.schema}.ingestion_state.last_root_workflow_job_id, EXCLUDED.last_root_workflow_job_id),
          updated_at=NOW()
        """
        with self._conn().cursor() as cur:
            cur.execute(sql, (template_id, root_id))
