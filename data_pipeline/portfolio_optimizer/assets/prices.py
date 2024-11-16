#!/usr/bin/env python
"""For the stocks passing the criteria, fetch the last X years of price data."""

import polars as pl
import yahooquery as yq
from dagster import asset

from ..resources.configs import Params
from ..resources.dbconn import PostgresConfig
from ..resources.dbtools import _append_data, _read_table
from ..resources.models import SecurityPrices


def _update_stock_prices(
    uri: str,
    piotroski_scores: pl.DataFrame,
    fundamentals: pl.DataFrame
    ) -> pl.DataFrame:
    """Download stock prices for stocks passing the criteria.

    Args:
        uri (str): The database URI
        piotroski_scores (pl.DataFrame): The Piotroski F-Score data
        fundamentals (pl.DataFrame): The updated fundamentals

    Returns:
        pl.DataFrame: The stock prices
    """

    # Get the configuration parameters
    params = Params()

    # Join symbol from fundamentals to piotroski_scores
    piotroski_scores = piotroski_scores.join(
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

    # Get updated price data
    price_data = _read_table(
        uri=uri,
        table_name='security_price'
    )

    return price_data


@asset(
    description="Download stock prices for stocks passing the criteria",
)
def stock_prices(
    piotroski_scores: pl.DataFrame,
    fundamentals: pl.DataFrame
    ) -> pl.DataFrame:
    """Download stock prices for stocks passing the criteria.

    Args:
        piotroski_scores (pl.DataFrame): The Piotroski F-Score data
        updated_fundamentals (pl.DataFrame): The updated fundamentals

    Returns:
        pl.DataFrame: The stock prices
    """

    # Initialize SSH tunnel to Postgres database
    pg_config = PostgresConfig()

    # Update security profiles, tunnel through SSH to access database
    prices = pg_config.tunneled(
        _update_stock_prices,
        piotroski_scores=piotroski_scores,
        fundamentals=fundamentals
    )

    return prices
