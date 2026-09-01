# Date-scoped ingestion change

Default execution selects the current calendar date in `run_timezone`. Supplying `run_date` in `YYYY-MM-DD` format selects an older date for deterministic backfill. AAP receives UTC start/end boundaries and PostgreSQL maintains checkpoint state per workflow template and date, preserving idempotence for both daily runs and historical reprocessing.
