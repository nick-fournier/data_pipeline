#!/usr/bin/env python
"""Module contains assets related to fundamentals data processing."""
from __future__ import annotations

import polars as pl
import yahooquery as yq
from dagster import asset

from data_pipeline.assets.downloader import download_stock_data
from data_pipeline.resources.configs import Fundamentals
from data_pipeline.resources.dbconn import PostgresConfig
from data_pipeline.resources.dbtools import _read_table
from data_pipeline.utils import camel_case


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

    field_map = {camel_case(k): k for k in Fundamentals.__annotations__}
    fundamentals_df = pl.DataFrame(
        yq_request.get_financial_data(
            types=list(field_map.keys()),
            frequency="q",
            trailing=True,
        ).reset_index(),
    )

    # Rename columns
    fundamentals_df = fundamentals_df.rename(field_map)

    # Coerce columns to correct data types
    for col in Fundamentals.__annotations__:
        pl_type = getattr(pl, Fundamentals.__annotations__[col].split(".")[1])
        fundamentals_df = fundamentals_df.with_columns(
            fundamentals_df[col].cast(pl_type),
        )

    return fundamentals_df


@asset(
    description="Update company fundamentals from Yahoo Finance",
    required_resource_keys={"postgres"},
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
