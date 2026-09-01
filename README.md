# AAP Workflow Ingestion

Enterprise-oriented proof of concept for recursively ingesting Ansible Automation Platform workflow execution telemetry into PostgreSQL through one idempotent Ansible module.

## Purpose

The module accepts a Workflow Job Template ID and discovers root workflow runs for one selected calendar date. When `run_date` is omitted, the module uses today's date in `run_timezone`. Supplying `run_date` allows an older date to be ingested or backfilled without changing the code.

For each root run the collector recursively follows every executed workflow node, regardless of environment count, domain count, nesting depth or future workflow expansion.

```text
Desired State Configuration workflow
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

## Date selection

Normal daily execution requires no date input:

```yaml
run_timezone: Asia/Kolkata
```

The module resolves the current date in that timezone and queries only workflow runs whose `started` timestamp falls inside that local calendar day.

Historical ingestion is explicit:

```yaml
run_date: '2026-08-28'
run_timezone: Asia/Kolkata
```

The date is converted to UTC boundaries before querying AAP. Checkpoint state is maintained independently for each workflow template and selected date, so a historical backfill cannot disturb today's ingestion position.

## Design principles

- One Ansible module invocation performs collection and persistence.
- No `shell`, `command`, SQL task, external helper script or task loop is used for ingestion.
- AAP pagination and date filtering are handled in Python.
- Nested workflow traversal is recursive and has no fixed fan-out assumption.
- PostgreSQL schema ownership is confined to the storage layer.
- Every destination entity uses PostgreSQL `ON CONFLICT` semantics.
- Checkpointing is per workflow template and per run date.
- Root workflow ingestion is transactional; a failed root run does not advance the date checkpoint.
- PostgreSQL advisory locking is scoped to workflow template plus run date.
- Check mode performs discovery and reports what would be ingested without modifying PostgreSQL.
- Full AAP payloads are retained in JSONB alongside normalized reporting columns.
- Every executed unified job payload is retained on the workflow node, including non-job node types.
- AAP GET requests use bounded retries for transient failures.
- Absolute related/pagination URLs are rejected if they leave the configured Controller origin.
- The collector stops at the first incomplete root workflow within the selected date so a later completed run cannot move the date checkpoint past an active run.

## Data collected

| Entity | Examples |
|---|---|
| Workflow runs | root/child relationship, depth, status, duration, inventory, launch type, raw payload |
| Workflow nodes | node identifier, parent workflow, executed unified job ID/type, full unified-job payload, raw node payload |
| Jobs | template, status, duration, inventory, execution environment, slicing, limit, parent workflow |
| Inventories | ID, name, organization, kind, variables, raw payload |
| Host summaries | changed, failures, ok, skipped, unreachable/dark, processed |
| Job events | host, play, task, action, changed/failed/unreachable, result JSON, complete raw event |
| Workflow metrics | workflow/job/host/event counts and high-level success/failure counters |
| Ingestion state | last successfully committed root workflow job per template and run date |

The normalized fields support reporting immediately, while raw JSONB preserves source fidelity for future analytics without requiring the collector to predict every AAP/module return field.

## Execution flow

```text
Workflow Job Template ID
        |
        v
selected run date
(default = today in run_timezone)
        |
        v
UTC start/end query window
        |
        v
root workflow jobs after that date's checkpoint
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
per-date checkpoint update after successful commit
```

## Example

```yaml
- name: Ingest AAP workflow telemetry
  hosts: localhost
  gather_facts: false

  tasks:
    - name: Recursively ingest workflow runs for selected date
      sudeep.aap_ingestion.aap_workflow_ingest:
        controller_url: "{{ controller_url }}"
        token: "{{ controller_token }}"
        workflow_template_id: "{{ workflow_template_id }}"
        postgres_dsn: "{{ postgres_dsn }}"
        postgres_schema: aap_ingest
        run_date: "{{ run_date | default(omit) }}"
        run_timezone: "{{ run_timezone | default('UTC') }}"
```

Normal run: omit `run_date` to ingest today's workflow runs. For historical backfill, supply an extra variable such as `run_date=2026-08-28`.

There are no database commands in the playbook. The module owns schema initialization, reads, upserts, transactions and checkpoint management.

## Runtime dependencies

The Execution Environment needs `requests >= 2.31` and `psycopg >= 3.1`. IANA timezone data must be available in the Execution Environment when non-UTC `run_timezone` values are used. Nothing is installed dynamically by the playbook.

## Scope

This POC captures complete workflow execution lineage, leaf job telemetry, host summaries, inventory metadata, task/job events and raw API payloads needed to build reporting views in PostgreSQL. Project updates, inventory updates and system jobs are retained as full unified-job JSON on execution nodes but are not yet normalized into dedicated tables.

Before production adoption, integration-test against the organization's supported AAP/PostgreSQL versions and define retention/partitioning policy for high-volume event data.
