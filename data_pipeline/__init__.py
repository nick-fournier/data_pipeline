#!/usr/bin/env python
"""Data pipeline definitions and resources."""

from dagster import Definitions, load_assets_from_modules

from data_pipeline.resources.dbconn import PostgresPolarsIOManager

from .assets import (
    fundamentals,
    scores,
    security_profiles,
    stock_listing,
)

all_assets = load_assets_from_modules([
        stock_listing,
        security_profiles,
        fundamentals,
        scores,
    ])

defs = Definitions(
    assets=all_assets,
    resources={
        # "io_manager": DuckDBPolarsIOManager(database=db_uri),
        # "postgres": PostgresConfig(),
        "pgio_manager": PostgresPolarsIOManager(),
    },
)
