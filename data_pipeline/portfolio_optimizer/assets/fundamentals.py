#!/usr/bin/env python
"""Module contains assets related to fundamentals data processing."""
from __future__ import annotations

import logging

import pandas as pd
import polars as pl
import yahooquery as yq
from dagster import asset

from ..resources.dbconn import PostgresConfig
from ..resources.dbtools import _read_table
from ..resources.models import Fundamentals
from ..utils import camel_case
from .downloader import download_stock_data
from .scores import PFScoreMeasures

logger = logging.getLogger(__name__)


VAL_MEASURE_FIELDS = [
    "enterprise_value",
    "enterprises_value_ebitda_ratio",
    "enterprises_value_revenue_ratio",
    "forward_pe_ratio",
    "market_cap",
    "pb_ratio",
    "pe_ratio",
    "peg_ratio",
    "ps_ratio",
]

SCORE_FIELDS = list(PFScoreMeasures.__annotations__.keys())

def _retrieve_outdated_fundamentals(
    uri: str,
    ) -> pl.DataFrame:
    """Retrieve outdated fundamentals.

    Get list of symbols that do not have data for the current fiscal quarter.
    Q1: Jan, Feb, Mar (1, 2, 3)
    Q2: Apr, May, Jun (4, 5, 6)
    Q3: Jul, Aug, Sep (7, 8, 9)
    Q4: Oct, Nov, Dec (10, 11, 12)
    """
    query = """
        SELECT * FROM (
            SELECT
                symbol,
                MAX(as_of_date),
                CASE
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (1, 2, 3) THEN 'Q1'
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (4, 5, 6) THEN 'Q2'
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (7, 8, 9) THEN 'Q3'
                    WHEN EXTRACT(MONTH FROM CURRENT_DATE) IN (10, 11, 12) THEN 'Q4'
                    END AS fiscal_quarter
            FROM fundamentals
            GROUP BY symbol
        )
        WHERE fiscal_quarter < (
        SELECT
            CASE
                WHEN EXTRACT(MONTH FROM NOW()) IN (1, 2, 3) THEN 'Q1'
                WHEN EXTRACT(MONTH FROM NOW()) IN (4, 5, 6) THEN 'Q2'
                WHEN EXTRACT(MONTH FROM NOW()) IN (7, 8, 9) THEN 'Q3'
                WHEN EXTRACT(MONTH FROM NOW()) IN (10, 11, 12) THEN 'Q4'
            END
        )
    """
    return pl.read_database_uri(query, uri)


def _retrieve_missing_fundamentals(
    uri: str,
    symbols: pl.Series,
    ) -> pl.DataFrame:
    """Retrieve missing fundamentals.

    Get list of symbols that do not have data in the fundamentals table.
    """
    _query = """
        SELECT symbol
        FROM (VALUES %s) AS v(symbol)
        WHERE symbol NOT IN (SELECT symbol FROM fundamentals f)
        """
    query = _query % ", ".join([f"('{s}')" for s in symbols])

    return pl.read_database_uri(query, uri)

def fetch_fundamentals(new_stocks: pl.DataFrame) -> pl.DataFrame:
    """Parse security profile from Yahoo Finance.

    This is function parses the security profile from Yahoo Finance into a DataFrame.

    Args:
    ----
        new_stocks (pl.DataFrame): The list of new stock symbols

    Returns:
    -------
        pl.DataFrame: The fundamentals DataFrame

    """
    yq_request = yq.Ticker(
        new_stocks["symbol"].to_list(),
        asynchronous=True,
        validate=True,
    )

    fndmtl_fields = {}
    valmes_fields = {}
    for k in Fundamentals.__annotations__:
        if k in VAL_MEASURE_FIELDS:
            valmes_fields[camel_case(k)] = k
        elif k not in SCORE_FIELDS and k not in VAL_MEASURE_FIELDS:
            fndmtl_fields[camel_case(k)] = k

    _valuation_measures_df = pl.DataFrame(
        yq_request.valuation_measures.reset_index(),
    )

    response = yq_request.get_financial_data(
            types=list(fndmtl_fields.keys()),
            frequency="q",
            trailing=True,
        )

    if not isinstance(response, pd.DataFrame):
        msg = f"Failed to fetch financial data from Yahoo Finance, skipping: {response}"
        logger.error(msg)
        return pl.DataFrame()

    _financials_df = pl.DataFrame(response.reset_index())

    # Join the two DataFrames and rename columns
    _fundamentals_df = _financials_df.join(
        _valuation_measures_df,
        on=["symbol", "asOfDate", "periodType"],
        how="left",
    ).rename({**fndmtl_fields, **valmes_fields}).lazy()

    # Validate the DataFrame
    return Fundamentals.validate(_fundamentals_df).collect() # type: ignore


@asset(
    description="Update company fundamentals from Yahoo Finance",
)
def updated_fundamentals(
    updated_security_profiles: pl.DataFrame,
) -> pl.DataFrame:
    """Update fundamentals database.

    This function updates the fundamentals database with the latest financial metrics.

    Args:
    ----
        updated_security_profiles (pl.DataFrame): The updated security profiles

    Returns:
    -------
        pl.DataFrame: The updated fundamentals

    """
    # Need to check symbol: date key pairs for any missing combinations.

    # Initialize SSH tunnel to Postgres database
    pg_config = PostgresConfig()

    # Get (possibly) outdated fundamentals (last updated more than 90 days ago)
    outdated_symbols = pg_config.tunneled(
        fn=_retrieve_outdated_fundamentals,
    )

    # Get symbols missing from fundamentals table (i.e. no data available)
    missing_symbols = pg_config.tunneled(
        fn=_retrieve_missing_fundamentals,
        symbols=updated_security_profiles["symbol"],
    )

    # Get list of symbols that are either outdated or missing
    to_be_updated = updated_security_profiles.filter(
        pl.col("symbol").is_in(outdated_symbols["symbol"])
        | pl.col("symbol").is_in(missing_symbols["symbol"]),
    )

    if not to_be_updated.is_empty():
        download_stock_data(
            pg_config = pg_config,
            new_stocks = to_be_updated,
            fetch_fn = fetch_fundamentals,
            output_table = "fundamentals",
            pk = ["symbol", "as_of_date", "period_type", "currency_code"],
        )

    # Get latest securities list
    return pg_config.tunneled(
        _read_table,
        table_name="fundamentals",
        )
