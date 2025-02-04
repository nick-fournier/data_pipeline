#!/usr/bin/env python
"""For the stocks passing the criteria, fetch the last X years of price data."""

import datetime
import random

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

    # All symbols with no price data
    all_symbols = pl.read_database_uri(
        query="SELECT DISTINCT symbol FROM security_price",
        uri=uri
    )['symbol']

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
        (pl.col('pf_score') > params.FSCORE_CUTOFF) & (
            (pl.col('symbol').is_in(ood_symbols)) |
            (~pl.col('symbol').is_in(all_symbols))
        )
    )['symbol'].unique()

    if symbols.shape[0] == 0:
        return

    # Get latest price data from Yahoo Finance
    remaining_symbols = symbols.to_list()
    consecutive_errors = 0
    chunk_size = 100
    max_errors = 10
    while len(remaining_symbols) > 0 and consecutive_errors < max_errors:

        # Choose 100 random symbols, pop from remaining symbols
        _symbols = random.sample(
            remaining_symbols,
            min(chunk_size, len(remaining_symbols)),
        )

        # Remove from remaining symbols
        for _symbol in _symbols:
            remaining_symbols.remove(_symbol)

        stock_data = yq.Ticker(_symbols)

        try:
            msg = (
                f"Fetching price data for {len(_symbols)} of "
                f"{len(remaining_symbols)} remaining symbols"
            )
            print(msg)

            pd_price_data = stock_data.history(
                period=params.PRICE_PERIOD,
                interval=params.PRICE_INTERVAL
            )
            pl_price_data = pl.DataFrame(
                pd_price_data.reset_index()
            )

            # Validate and trim price data
            latest_price_data = pl_price_data.select(
                SecurityPrices.__annotations__.keys(),
            )
            latest_price_data: pl.DataFrame = SecurityPrices.validate(
                latest_price_data, # type: ignore
            )

            # Append new price data to db
            _append_data(
                uri=uri,
                table_name='security_price',
                new_data=latest_price_data,
                pk=['symbol', 'date']
            )

        except Exception as e:
            # Put back into remaining symbols to retry
            remaining_symbols.extend(_symbols)
            msg = f"Error {consecutive_errors}/{max_errors} fetching price data."
            print(msg)
            consecutive_errors += 1

            # If less than 100 left, reduce chunk size by half, minimum 1
            if len(remaining_symbols) < chunk_size:
                chunk_size = max(chunk_size // 2, 1)


