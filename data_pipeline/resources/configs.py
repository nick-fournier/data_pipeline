#!/usr/bin/env python
"""Module contains resources and configurations for the data pipeline."""
from __future__ import annotations

from datetime import datetime  # noqa: TCH003
from typing import TYPE_CHECKING

from dagster import Config
from pydantic import Field

if TYPE_CHECKING:
    import polars as pl

class Params(Config):
    """Configuration for the security screen.

    This configuration is used to specify the criteria for screening stock symbols.

    """

    MAX_SYMBOL_LENGTH: int = 5
    MIN_MARKET_CAP: int = 0
    MIN_VOLUME: int = 0
    MAX_FAILURES: int = 5
    CHUNK_SIZE: int = 500


class SecurityList(Config):
    """Configuration for the security list.

    This configuration is used to specify the security list to fetch stock symbols from.

    """

    symbol: pl.String = Field(description="The stock symbol")
    name: pl.String = Field(description="The name of the company", default=None)
    country: pl.String = Field(description="The country where the company is based", default=None)
    sector: pl.String = Field(description="The sector the company operates in", default=None)
    industry: pl.String = Field(description="The industry the company operates in", default=None)
    fulltime_employees: pl.Int32 = Field(
        description="The number of full-time employees at the company",
        default=None,
        )
    business_summary: pl.String = Field(
        description="A summary of the company's business",
        default=None,
        )
    last_updated: datetime = Field(description="The date the data was last updated", default=None)


class Fundamentals(Config):
    """Configuration for the fundamentals.

    This configuration is used to specify the financial metrics to fetch from the Yahoo Finance API.

    """

    symbol: pl.String = Field(description="The stock symbol")
    period_type: pl.String = Field(description="The period type")
    as_of_date: pl.Date = Field(description="The date the data was last updated", default=None)
    net_income: pl.Int64 = Field(description="The net income of the company", default=None)
    net_income_common_stockholders: pl.Int64 = Field(
        description="The net income available to common stockholders",
        default=None,
        )
    total_liabilities_net_minority_interest: pl.Int64 = Field(
        description="The total liabilities of the company",
        default=None,
        )
    total_assets: pl.Int64 = Field(description="The total assets of the company", default=None)
    current_assets: pl.Int64 = Field(description="The current assets of the company", default=None)
    current_liabilities: pl.Int64 = Field(
        description="The current liabilities of the company",
        default=None,
        )
    capital_stock: pl.Int64 = Field(
        description="The number of shares outstanding",
        default=None,
        )
    cash_and_cash_equivalents: pl.Int64 = Field(
        description="The cash and cash equivalents of the company",
        default=None,
        )
    gross_profit: pl.Int64 = Field(description="The gross profit of the company", default=None)
    total_revenue: pl.Int64 = Field(description="The total revenue of the company", default=None)
    currency_code: pl.String = Field(description="Currency code")
