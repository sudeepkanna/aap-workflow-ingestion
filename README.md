# AAP Workflow Ingestion

Enterprise-oriented proof of concept for recursively ingesting Ansible Automation Platform workflow execution telemetry into PostgreSQL through a single idempotent Ansible module.

The collection starts from a Workflow Job Template ID, discovers root workflow runs, recursively traverses nested workflow jobs of arbitrary depth and fan-out, ingests child jobs, host summaries, task/job events, inventory metadata, parent-child relationships and raw API payloads, and maintains an incremental checkpoint.

All controller access, recursion, PostgreSQL schema management, transactions, upserts and checkpointing are implemented inside Python module/module_utils code. The example playbook does not execute shell, command, SQL or helper scripts.

See the implementation branch/PR for the complete POC.