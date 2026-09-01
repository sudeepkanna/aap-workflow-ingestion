#!/usr/bin/python
from __future__ import annotations

DOCUMENTATION = r'''
---
module: aap_workflow_ingest
short_description: Recursively ingest AAP workflow execution telemetry into PostgreSQL
description:
  - Starts from an AAP Workflow Job Template ID.
  - By default ingests workflow runs started today in the configured timezone.
  - Accepts run_date for idempotent ingestion or backfill of an older date.
  - Recursively traverses nested workflow jobs with no fixed depth or fan-out assumptions.
  - Persists workflow relationships, jobs, inventories, host summaries, job events and raw JSON payloads.
  - Uses PostgreSQL upserts and a per-template, per-date checkpoint for idempotence.
  - Supports check mode without modifying PostgreSQL.
options:
  controller_url:
    type: str
    required: true
  token:
    type: str
    required: true
    no_log: true
  workflow_template_id:
    type: int
    required: true
  postgres_dsn:
    type: str
    required: true
    no_log: true
  postgres_schema:
    type: str
    default: aap_ingest
  run_date:
    type: str
    required: false
    description:
      - Workflow start date to ingest in YYYY-MM-DD format.
      - When omitted, today's date is used in run_timezone.
  run_timezone:
    type: str
    default: UTC
    description:
      - IANA timezone used to define the selected day's boundaries.
  api_path:
    type: str
    default: /api/v2
  verify_ssl:
    type: bool
    default: true
  request_timeout:
    type: int
    default: 30
  page_size:
    type: int
    default: 200
  max_root_runs:
    type: int
    default: 0
    description:
      - Maximum root workflow runs to process in one invocation.
      - Zero means no module-imposed limit.
requirements:
  - requests
  - psycopg >= 3.1
supports_check_mode: true
'''

EXAMPLES = r'''
- name: Ingest today's AAP workflow telemetry
  sudeep.aap_ingestion.aap_workflow_ingest:
    controller_url: https://aap.example.com
    token: "{{ controller_token }}"
    workflow_template_id: 42
    postgres_dsn: "{{ aap_reporting_postgres_dsn }}"
    run_timezone: Asia/Kolkata

- name: Backfill a historical date
  sudeep.aap_ingestion.aap_workflow_ingest:
    controller_url: https://aap.example.com
    token: "{{ controller_token }}"
    workflow_template_id: 42
    postgres_dsn: "{{ aap_reporting_postgres_dsn }}"
    run_date: '2026-08-28'
    run_timezone: Asia/Kolkata
'''

RETURN = r'''
processed_root_workflow_ids:
  description: Root workflow job IDs committed during this invocation.
  returned: always
  type: list
run_date:
  description: Date selected for ingestion.
  returned: always
  type: str
window_start_utc:
  returned: always
  type: str
window_end_utc:
  returned: always
  type: str
checkpoint_before:
  returned: always
  type: int
checkpoint_after:
  returned: always
  type: int
metrics:
  description: Per-root ingestion metrics.
  returned: when roots are processed
  type: dict
planned_root_workflow_ids:
  description: Root workflow IDs that would be processed in check mode.
  returned: check mode
  type: list
'''

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.aap_client import AAPClient, AAPClientError
from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.collector import WorkflowCollector, plan_roots
from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.date_aware_store import DateAwarePostgresStore
from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.postgres_store import StoreError


TERMINAL_STATUSES = {"successful", "failed", "error", "canceled"}


def argument_spec():
    return {
        "controller_url": {"type": "str", "required": True},
        "token": {"type": "str", "required": True, "no_log": True},
        "workflow_template_id": {"type": "int", "required": True},
        "postgres_dsn": {"type": "str", "required": True, "no_log": True},
        "postgres_schema": {"type": "str", "default": "aap_ingest"},
        "run_date": {"type": "str", "required": False, "default": None},
        "run_timezone": {"type": "str", "default": "UTC"},
        "api_path": {"type": "str", "default": "/api/v2"},
        "verify_ssl": {"type": "bool", "default": True},
        "request_timeout": {"type": "int", "default": 30},
        "page_size": {"type": "int", "default": 200},
        "max_root_runs": {"type": "int", "default": 0},
    }


def resolve_run_window(run_date_value: str | None, timezone_name: str):
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid run_timezone '{timezone_name}'") from exc

    try:
        selected_date = date.fromisoformat(run_date_value) if run_date_value else datetime.now(tz).date()
    except ValueError as exc:
        raise ValueError("run_date must use YYYY-MM-DD format") from exc

    local_start = datetime.combine(selected_date, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_end.astimezone(timezone.utc)
    return selected_date, utc_start.isoformat(), utc_end.isoformat()


def main():
    module = AnsibleModule(argument_spec=argument_spec(), supports_check_mode=True)
    p = module.params
    template_id = p["workflow_template_id"]

    try:
        selected_date, window_start, window_end = resolve_run_window(
            p["run_date"], p["run_timezone"]
        )
    except ValueError as exc:
        module.fail_json(msg=str(exc))

    client = AAPClient(
        base_url=p["controller_url"],
        token=p["token"],
        api_path=p["api_path"],
        verify_ssl=p["verify_ssl"],
        timeout=p["request_timeout"],
        page_size=p["page_size"],
    )
    store = DateAwarePostgresStore(p["postgres_dsn"], p["postgres_schema"])

    processed = []
    metrics = {}
    lock_acquired = False

    try:
        store.connect()
        schema_existed = store.schema_exists()
        checkpoint_before = store.checkpoint_for_date(template_id, selected_date)
        roots = plan_roots(
            client,
            template_id=template_id,
            started_gte=window_start,
            started_lt=window_end,
            after_id=checkpoint_before,
            limit=p["max_root_runs"],
        )

        completed_roots = []
        blocked_by = None
        for root in roots:
            if root.get("status") not in TERMINAL_STATUSES:
                blocked_by = {"id": root.get("id"), "status": root.get("status")}
                break
            completed_roots.append(root)

        common_result = {
            "run_date": selected_date.isoformat(),
            "run_timezone": p["run_timezone"],
            "window_start_utc": window_start,
            "window_end_utc": window_end,
            "checkpoint_before": checkpoint_before,
            "blocked_by_incomplete_root": blocked_by,
        }

        if module.check_mode:
            module.exit_json(
                changed=bool((not schema_existed) or completed_roots),
                checkpoint_after=checkpoint_before,
                planned_root_workflow_ids=[int(r["id"]) for r in completed_roots],
                **common_result,
            )

        if not store.acquire_lock(template_id):
            module.fail_json(msg=f"Another ingestion process is already active for workflow template {template_id}")
        lock_acquired = True

        store.ensure_schema()
        collector = WorkflowCollector(client, store)

        for root in completed_roots:
            root_id = int(root["id"])
            with store.root_transaction():
                metrics[str(root_id)] = collector.collect_root(root)
                store.update_checkpoint_for_date(template_id, selected_date, root_id)
            processed.append(root_id)

        checkpoint_after = processed[-1] if processed else checkpoint_before
        module.exit_json(
            changed=bool((not schema_existed) or processed),
            processed_root_workflow_ids=processed,
            checkpoint_after=checkpoint_after,
            metrics=metrics,
            **common_result,
        )

    except (AAPClientError, StoreError, KeyError, ValueError) as exc:
        module.fail_json(msg=str(exc), processed_root_workflow_ids=processed)
    except Exception as exc:
        module.fail_json(msg=f"AAP workflow ingestion failed: {exc}", processed_root_workflow_ids=processed)
    finally:
        if lock_acquired:
            try:
                store.release_lock(template_id)
            except Exception:
                pass
        store.close()
        client.close()


if __name__ == "__main__":
    main()
