#!/usr/bin/env python
"""For the stocks passing the criteria, fetch the last X years of price data."""

import datetime

import polars as pl
import yahooquery as yq
from dagster import asset

from data_pipeline.portfolio_optimizer.resources.configs import Params
from data_pipeline.portfolio_optimizer.resources.dbtools import _append_data
from data_pipeline.portfolio_optimizer.resources.models import SecurityPrices
from data_pipeline.portfolio_optimizer.utils import quarter_dates


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

    # Get last quarter date
    now = datetime.datetime.now()
    year = (now - datetime.timedelta(days=90)).year
    quarter = ((now.month // 3) - 1) % 4 + 1
    _, qend = quarter_dates(year, quarter)

    # Join symbol from fundamentals to piotroski_scores
    piotroski_scores = scores.join(
        fundamentals.select(['id', 'symbol']),
        left_on='fundamentals_id',
        right_on='id',
        how='inner'
    )

    # Filter out symbols with no price data past last quarter end date
    query = f"""
        SELECT symbol
        FROM security_price
        GROUP BY symbol
        HAVING MAX(date) <= '{qend}'
    """
    ood_symbols = pl.read_database_uri(
        query=query,
        uri=uri
    )['symbol'].unique()


    # Get symbols for out-of-date stocks passing the criteria
    symbols = piotroski_scores.filter(
        (pl.col('pf_score') > params.FSCORE_CUTOFF) &
        (pl.col('symbol').is_in(ood_symbols))
    )['symbol'].unique()

    # Get latest price data from Yahoo Finance
    stock_data = yq.Ticker(symbols.to_list())

    pd_price_data = stock_data.history(
            period=params.PRICE_PERIOD,
            interval=params.PRICE_INTERVAL
        )

    latest_price_data = pl.DataFrame(
        pd_price_data.reset_index()
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
