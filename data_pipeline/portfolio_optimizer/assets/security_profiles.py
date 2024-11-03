#!/usr/bin/env python
"""Module contains assets related to stock data processing."""

import logging
from datetime import datetime, timezone

import polars as pl
import yahooquery as yq
from dagster import asset

from ..resources.dbconn import PostgresConfig
from ..resources.dbtools import (
    _read_table,
    _remove_stocks,
)
from ..resources.models import SecurityList
from ..utils import snake_case
from .downloader import download_stock_data

logger = logging.getLogger(__name__)


def fetch_profiles(
    new_stocks: pl.DataFrame,
    ) -> pl.DataFrame:
    """Parse security profile from Yahoo Finance.

    This is function parses the security profile from Yahoo Finance into a DataFrame.

    Args:
    ----
        yq_request (yq.Ticker): The YahooQuery Ticker object
        new_stocks (pl.DataFrame): The list of new stock symbols

    Returns:
    -------
        pl.DataFrame: The security profile DataFrame

    """
    yq_request = yq.Ticker(
        new_stocks["symbol"].to_list(),
        asynchronous=True,
        validate=True,
    )

    profiles_ls = []
    for symbol, meta_data in yq_request.asset_profile.items():
        if not isinstance(meta_data, dict) or not symbol:
            continue

        _meta_data = {
            **{snake_case(k): v for k, v in meta_data.items()},
            "symbol": symbol,
            "name": new_stocks.filter(pl.col("symbol") == symbol)["name"][0],
            "last_updated": datetime.now(timezone.utc),
        }
        profiles_ls.append(SecurityList(**_meta_data))

    return pl.DataFrame(profiles_ls)


@asset(
    description="Update security profiles from Yahoo Finance",
)
def updated_security_profiles(
    stock_listings: pl.DataFrame,
) -> pl.DataFrame:
    """Update security list database.

    This function updates and returns the latest securities list
    from the Postgres database.

    Args:
    ----
        stock_listings (pl.DataFrame): The latest stock symbols from the NASDAQ screener.

    Returns:
    -------
        pl.DataFrame: The updated securities list

    """
    # Initialize SSH tunnel to Postgres database
    pg_config = PostgresConfig()

    # New stocks to add
    existing_securities_df = pg_config.tunneled(
        _read_table,
        table_name="security_list",
    )
    new_stocks = stock_listings.filter(
        ~pl.col("symbol").is_in(existing_securities_df["symbol"]),
    )

    # Stocks to remove
    removed_stocks = existing_securities_df.filter(
        ~pl.col("symbol").is_in(stock_listings["symbol"]),
    )["symbol"]

    if not removed_stocks.is_empty():
        pg_config.tunneled(
            _remove_stocks,
            table_name="security_list",
            removed_stocks=removed_stocks,
            )

    if not new_stocks.is_empty():
        logger.info(
            f"Downloading {new_stocks.shape[0]} stock profiles",
        )

        download_stock_data(
            pg_config = pg_config,
            new_stocks = new_stocks,
            fetch_fn = fetch_profiles,
            output_table = "security_list",
        )

    # Get latest securities list
    return pg_config.tunneled(
        _read_table,
        table_name="security_list",
        )
