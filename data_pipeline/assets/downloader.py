#!/usr/bin/env python
"""Module contains assets related to downloading stock data."""
from __future__ import annotations

import logging
from typing import Callable

import polars as pl  # noqa: TCH002
from tqdm import tqdm

from data_pipeline.resources.configs import Params
from data_pipeline.resources.dbconn import PostgresConfig  # noqa: TCH001
from data_pipeline.resources.dbtools import _append_new_data

logger = logging.getLogger(__name__)

def download_stock_data(
    pg_config: PostgresConfig,
    new_stocks: pl.DataFrame,
    fetch_fn: Callable,
    output_table: str,
    pk: list[str] | None = None,
    ) -> None:
    """Download stock profile from Yahoo Finance.

    Downloads the security profile and fundamentals for a list of stock symbols
    and appends the metadata to the existing security list in the Postgres database.

    Args:
    ----
        pg_config (PostgresConfig): The Postgres configuration
        new_stocks (pl.DataFrame): The list of new stock symbols
        existing_stocks (pl.DataFrame): The existing stock symbols
        fetch_fn (Callable(yq_request, new_stocks)): The fetch function to download the data
        output_table (str): The output table name
        chunk_size (int, optional): The size of the chunks to split the
            list of symbols into. Defaults to 100.
        pk (list[str], optional): The primary key column(s) to use for deduplication.

    Returns:
    -------
        None

    """
    params = Params()

    # Split symbols into chunks
    _iter = new_stocks.iter_slices(params.CHUNK_SIZE)

    for _chunk in tqdm(_iter, total=len(new_stocks) // params.CHUNK_SIZE):
        try:
            downloaded_df = fetch_fn(_chunk)

            if downloaded_df.is_empty():
                continue

        except ValueError as e:
            err = f"Error: {e}"
            logger.exception(err)

        pg_config.tunneled(
            _append_new_data,
            table_name=output_table,
            new_data=downloaded_df,
            pk=pk,
            )
