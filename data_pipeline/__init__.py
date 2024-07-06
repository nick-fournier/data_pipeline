#!/usr/bin/env python
"""Data pipeline definitions and resources."""

from dagster import Definitions, load_assets_from_modules

# from dagster_duckdb_polars import DuckDBPolarsIOManager  # noqa: ERA001
from .assets import (
    fundamentals,
    scores,
    security_profiles,
    stock_listing,
)
from .resources.dbconn import PostgresConfig

all_assets = load_assets_from_modules([
        stock_listing,
        security_profiles,
        fundamentals,
        scores,
    ])

defs = Definitions(
    assets=all_assets,
    resources={
        # "io_manager": DuckDBPolarsIOManager(database="data_pipeline.db"),  # noqa: ERA001
        "postgres": PostgresConfig(),
    },
)
