# AAP Workflow Ingestion

Enterprise-oriented proof of concept for recursively ingesting Ansible Automation Platform workflow execution telemetry into PostgreSQL through one idempotent Ansible module.

## Purpose

The module accepts a Workflow Job Template ID and discovers completed root workflow runs. For each root run it recursively follows every executed workflow node, regardless of environment count, domain count, nesting depth or future workflow expansion.

A topology such as the following requires no code changes:

```text
workflow
├── DT workflow
│   ├── domain-1 job
│   └── domain-2 job
├── ST workflow
├── ET workflow
└── PR workflow
    ├── domain-1 job
    ├── domain-2 job
    ├── domain-3 job
    └── domain-4 job
```

If four environments later become eight, or four domains become ten, traversal remains data-driven because the module follows the AAP execution graph rather than hard-coding names or counts.

## Design principles

- One Ansible module invocation performs collection and persistence.
- No `shell`, `command`, SQL task, external helper script or task loop is used for ingestion.
- AAP pagination is handled in Python.
- Nested workflow traversal is recursive and has no fixed fan-out assumption.
- PostgreSQL schema creation and migration baseline are owned by the module's storage layer.
- Every destination entity is written with PostgreSQL `ON CONFLICT` semantics.
- A per-template checkpoint makes repeated runs incremental and idempotent.
- Root workflow ingestion is transactional; a failed root run does not advance the checkpoint.
- A PostgreSQL advisory lock prevents concurrent collectors for the same workflow template.
- Check mode performs discovery and reports what would be ingested without modifying PostgreSQL.
- Full AAP payloads are retained in JSONB alongside normalized reporting columns.
- The collector stops at the first incomplete root workflow so a later completed run cannot move the checkpoint past an active run.

## Data collected

The module persists:

| Entity | Examples |
|---|---|
| Workflow runs | root/child relationship, depth, status, duration, inventory, launch type, raw payload |
| Workflow nodes | node identifier, parent workflow, executed unified job ID/type, raw node payload |
| Jobs | template, status, duration, inventory, execution environment, slicing, limit, parent workflow |
| Inventories | ID, name, organization, kind, variables, raw payload |
| Host summaries | changed, failures, ok, skipped, unreachable/dark, processed |
| Job events | host, play, task, action, changed/failed/unreachable, result JSON, complete raw event |
| Workflow metrics | workflow/job/host/event counts and high-level success/failure counters |
| Ingestion state | last successfully committed root workflow job per template |

The normalized fields support reporting immediately, while the raw JSONB columns preserve source fidelity for future analytics without requiring the collector to predict every AAP/module return field.

## Collection layout

```text
plugins/
├── modules/
│   └── aap_workflow_ingest.py
└── module_utils/
    ├── aap_client.py
    ├── collector.py
    └── postgres_store.py
```

`aap_workflow_ingest.py` is deliberately thin. API access, graph traversal and persistence are separated into focused module utilities so the Ansible interface remains stable as the implementation evolves.

## Execution flow

```text
Workflow Job Template ID
        |
        v
completed root workflow jobs after checkpoint
        |
        v
workflow nodes
        |
        +--> child workflow_job --> recurse
        |
        +--> child job ----------> job metadata
                                      |
                                      +--> inventory
                                      +--> host summaries
                                      +--> job events/tasks/results
        |
        v
PostgreSQL transaction per root workflow
        |
        v
checkpoint update only after successful commit
```

## Example

```yaml
- name: Ingest AAP workflow telemetry
  hosts: localhost
  gather_facts: false

  tasks:
    - name: Recursively ingest completed workflow runs
      sudeep.aap_ingestion.aap_workflow_ingest:
        controller_url: "{{ controller_url }}"
        token: "{{ controller_token }}"
        workflow_template_id: "{{ workflow_template_id }}"
        postgres_dsn: "{{ postgres_dsn }}"
        postgres_schema: aap_ingest
```

There are no database commands in the playbook. The module owns schema initialization, reads, upserts, transactions and checkpoint management.

## AAP API path

The default API base is `/api/v2`. Environments exposing controller resources through the AAP platform gateway can override it, for example:

```yaml
api_path: /api/controller/v2
```

## Runtime dependencies

The Execution Environment running the module needs:

```text
requests >= 2.31
psycopg >= 3.1
```

These dependencies belong in the Execution Environment image. Nothing is installed dynamically by the playbook.

## PostgreSQL

The target database and login must already exist and the login must be permitted to create/use the configured schema. The module creates and maintains its own tables inside that schema. The POC does not execute external `.sql` files.

Default schema: `aap_ingest`.

## Check mode

```bash
ansible-playbook playbooks/ingest.yml --check
```

Check mode queries AAP and, when available, reads the existing checkpoint. It reports the root workflow job IDs that would be processed without creating schemas, tables or rows.

## Idempotence

Idempotence is provided at several levels:

1. Workflow-template checkpoint prevents unnecessary historical re-fetching.
2. Workflow, node, job, inventory, host-summary and event IDs are destination keys.
3. Re-seen records use `ON CONFLICT ... DO UPDATE` rather than duplicate inserts.
4. A root workflow is committed atomically with its checkpoint.
5. Incomplete root workflows are not checkpointed.

## Scope of this POC

This version intentionally focuses on workflow execution telemetry required for operational reporting. Project updates, inventory updates and system jobs that may appear as workflow nodes are preserved in `execution_nodes` but are not yet expanded into dedicated telemetry tables. The execution-node model is designed so those unified-job types can be added without changing recursive workflow discovery.

## Enterprise hardening path

Before production adoption, add integration tests against the organization's supported AAP/PostgreSQL versions, credential injection through AAP credentials rather than plain variables, TLS policy validation, retention/partitioning policy for high-volume event tables, observability for collector failures and throughput, and controlled schema-version migrations.
