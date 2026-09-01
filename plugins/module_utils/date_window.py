from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def resolve_run_window(
    run_date_value: Optional[str],
    timezone_name: str,
    now: Optional[datetime] = None,
) -> Tuple[date, str, str]:
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Invalid run_timezone '{timezone_name}'") from exc

    try:
        reference = now.astimezone(tz) if now else datetime.now(tz)
        selected_date = date.fromisoformat(run_date_value) if run_date_value else reference.date()
    except ValueError as exc:
        raise ValueError("run_date must use YYYY-MM-DD format") from exc

    local_start = datetime.combine(selected_date, time.min, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    return (
        selected_date,
        local_start.astimezone(timezone.utc).isoformat(),
        local_end.astimezone(timezone.utc).isoformat(),
    )
