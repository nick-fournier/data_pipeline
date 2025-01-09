#!/usr/bin/env python
"""For the stocks passing the criteria, fetch the last X years of price data."""

import polars as pl
import yahooquery as yq
from dagster import asset

from ..resources.configs import Params
from ..resources.dbtools import _append_data
from ..resources.models import SecurityPrices


@asset(
    description="Download stock prices for stocks passing the criteria",
    io_manager_key="pgio_manager",
)
def security_price(
    context,
    scores: pl.DataFrame,
    fundamentals: pl.DataFrame
    ) -> None:
    """Download stock prices for stocks passing the criteria.

    Args:
    ----
        context (Context): The Dagster context
        scores (pl.DataFrame): The Piotroski F-Score data
        fundamentals (pl.DataFrame): The updated fundamentals

    Returns:
        pl.DataFrame: The stock prices
    """

    # Get database URI
    uri = context.resources.pgio_manager.postgres_config.db_uri()

    # Get the configuration parameters
    params = Params()

    # Join symbol from fundamentals to piotroski_scores
    piotroski_scores = scores.join(
        fundamentals.select(['id', 'symbol']),
        left_on='fundamentals_id',
        right_on='id',
        how='inner'
    )

    # Get symbols for stocks passing the criteria
    symbols = piotroski_scores.filter(
        pl.col('pf_score') > params.FSCORE_CUTOFF
    )['symbol'].unique()

    # Get latest price data from Yahoo Finance
    stock_data = yq.Ticker(symbols.to_list())
    latest_price_data = pl.DataFrame(
        stock_data.history(
            period=params.PRICE_PERIOD,
            interval=params.PRICE_INTERVAL
        ).reset_index()
    )

    # Validate and trim price data
    latest_price_data = latest_price_data.select(SecurityPrices.__annotations__.keys())
    latest_price_data: pl.DataFrame = SecurityPrices.validate(latest_price_data) # type: ignore

    # Append new price data to db
    _append_data(
        uri=uri,
        table_name='security_price',
        new_data=latest_price_data,
        pk=['symbol', 'date']
    )
