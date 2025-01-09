#!/usr/bin/env python
"""Data pipeline definitions and resources."""


from dagster import Definitions, load_assets_from_modules

from data_pipeline.portfolio_optimizer.assets import (
    forecast_returns,
    fundamentals,
    prices,
    scores,
    security_profiles,
    stock_listing,
)
from data_pipeline.portfolio_optimizer.resources import dbconn

all_assets = load_assets_from_modules([
        stock_listing,
        security_profiles,
        fundamentals,
        scores,
        prices,
        forecast_returns,
    ])

pg_config = dbconn.PostgresConfig()
pg_io_manager = dbconn.PostgresPolarsIOManager(postgres_config=pg_config)

defs = Definitions(
    assets=all_assets,
    resources={
        # "io_manager": DuckDBPolarsIOManager(database=db_uri),
        # "postgres": PostgresConfig(),
        "pgio_manager": pg_io_manager,
    },
)
