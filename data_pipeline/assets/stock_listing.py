#!/usr/bin/env python
"""Module contains assets related to stock data processing."""
import polars as pl
from dagster import asset

from data_pipeline.resources.configs import Params


@asset(
    description="""Fetches the latest stock symbols from the NASDAQ
    screener of NYSE, AMEX, and NASDAQ stocks""",
    )
def stock_listings() -> pl.DataFrame:
    """Fetch latest stock screener from NASDAQ.

    Fetches the latest stock market screener list from
    NASDAQ for NYSE, AMEX, and NASDAQ stocks.

    Returns
    -------
    pl.DataFrame: The latest stock symbols.

    """
    stock_list = (
        pl.read_csv(
            "data_pipeline/fixtures/nasdaq_screener_1718944810995.csv",
        )
        .rename(str.strip)
        .rename(str.lower)
    )
    # Eventually make this configurable
    params = Params()

    _filter = [
        # Filter out stocks with no market cap
        pl.col("market cap").is_not_null(),
        pl.col("market cap") > params.MIN_MARKET_CAP,
        # Filter out stocks with no volume
        pl.col("volume").is_not_null(),
        pl.col("volume") > params.MIN_VOLUME,
        # Filter out stocks with non letters in the symbol
        pl.col("symbol").str.contains(r"^[A-Z]"),
        # Filter out stocks with >= 5 letters in the symbol
        pl.col("symbol").str.lengths() < params.MAX_SYMBOL_LENGTH,
    ]

    return stock_list.filter(pl.all_horizontal(_filter))
