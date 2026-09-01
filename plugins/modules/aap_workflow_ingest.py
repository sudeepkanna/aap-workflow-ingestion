#!/usr/bin/python
from __future__ import annotations

DOCUMENTATION = r'''
---
module: aap_workflow_ingest
short_description: Recursively ingest AAP workflow execution telemetry into PostgreSQL
description:
  - Starts from an AAP Workflow Job Template ID.
  - Discovers completed root workflow runs incrementally.
  - Recursively traverses nested workflow jobs with no fixed depth or fan-out assumptions.
  - Persists workflow relationships, jobs, inventories, host summaries, job events and raw JSON payloads.
  - Uses PostgreSQL upserts and a per-template checkpoint for idempotence.
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
      - Maximum completed root workflow runs to process in one invocation.
      - Zero means no module-imposed limit.
requirements:
  - requests
  - psycopg >= 3.1
supports_check_mode: true
'''

EXAMPLES = r'''
- name: Ingest AAP workflow telemetry
  sudeep.aap_ingestion.aap_workflow_ingest:
    controller_url: https://aap.example.com
    token: "{{ controller_token }}"
    workflow_template_id: 42
    postgres_dsn: "{{ aap_reporting_postgres_dsn }}"
'''

RETURN = r'''
processed_root_workflow_ids:
  description: Root workflow job IDs committed during this invocation.
  returned: always
  type: list
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

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.aap_client import AAPClient, AAPClientError
from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.collector import WorkflowCollector, plan_roots
from ansible_collections.sudeep.aap_ingestion.plugins.module_utils.postgres_store import PostgresStore, StoreError


TERMINAL_STATUSES = {"successful", "failed", "error", "canceled"}


def argument_spec():
    return {
        "controller_url": {"type": "str", "required": True},
        "token": {"type": "str", "required": True, "no_log": True},
        "workflow_template_id": {"type": "int", "required": True},
        "postgres_dsn": {"type": "str", "required": True, "no_log": True},
        "postgres_schema": {"type": "str", "default": "aap_ingest"},
        "api_path": {"type": "str", "default": "/api/v2"},
        "verify_ssl": {"type": "bool", "default": True},
        "request_timeout": {"type": "int", "default": 30},
        "page_size": {"type": "int", "default": 200},
        "max_root_runs": {"type": "int", "default": 0},
    }


def main():
    module = AnsibleModule(argument_spec=argument_spec(), supports_check_mode=True)
    p = module.params
    template_id = p["workflow_template_id"]

    client = AAPClient(
        base_url=p["controller_url"],
        token=p["token"],
        api_path=p["api_path"],
        verify_ssl=p["verify_ssl"],
        timeout=p["request_timeout"],
        page_size=p["page_size"],
    )
    store = PostgresStore(p["postgres_dsn"], p["postgres_schema"])

    processed = []
    metrics = {}
    lock_acquired = False

    try:
        store.connect()
        schema_existed = store.schema_exists()
        checkpoint_before = store.checkpoint(template_id) if schema_existed else 0
        roots = plan_roots(
            client,
            template_id=template_id,
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

        if module.check_mode:
            module.exit_json(
                changed=bool((not schema_existed) or completed_roots),
                checkpoint_before=checkpoint_before,
                checkpoint_after=checkpoint_before,
                planned_root_workflow_ids=[int(r["id"]) for r in completed_roots],
                blocked_by_incomplete_root=blocked_by,
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
                store.update_checkpoint(template_id, root_id)
            processed.append(root_id)

        checkpoint_after = processed[-1] if processed else checkpoint_before
        module.exit_json(
            changed=bool((not schema_existed) or processed),
            processed_root_workflow_ids=processed,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            metrics=metrics,
            blocked_by_incomplete_root=blocked_by,
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
