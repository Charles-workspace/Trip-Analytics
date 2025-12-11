from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Dict

from snowflake.snowpark import Session

def copy_into_table(
    session: Session,
    table_name: str,
    stage_name: str,
    file_format_type: str,
    pattern: Optional[str] = None,
    file_format_options: Optional[Dict[str, str]] = None,
    match_by_column_name: str = "CASE_INSENSITIVE",
) -> None:
    """
    Generic COPY INTO helper that assumes files are ALREADY in the stage.
    Example stage_name: 'INBOUND_INTEGRATION.LANDING_TRIP.LANDING_TRIP_STAGE'
    """
    stage_ref = f"@{stage_name}"

    ff_opts = [f"TYPE = {file_format_type.upper()}"]
    if file_format_options:
        for k, v in file_format_options.items():
            ff_opts.append(f"{k}={v}")

    ff_clause = ", ".join(ff_opts)
    pattern_clause = f"PATTERN = '{pattern}'" if pattern else ""

    sql = f"""
        COPY INTO {table_name}
        FROM {stage_ref}
        FILE_FORMAT = ({ff_clause})
        MATCH_BY_COLUMN_NAME = {match_by_column_name}
        {pattern_clause}
    """
    print(f"Running COPY INTO for {table_name} from {stage_ref}")
    # print(sql) 
    session.sql(sql).collect()