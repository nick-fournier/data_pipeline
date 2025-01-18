
from datetime import datetime

import pandera.polars as pa
import polars as pl
import pydantic as pdt
from dagster import Config


class SecurityList(Config):
    """Configuration for the security list.

    This configuration is used to specify the security list to fetch stock symbols from.

    """

    symbol: str = pdt.Field(
        description="The stock symbol",
        )
    name: str = pdt.Field(
        description="The name of the company",
        default=None,
        )
    country: str = pdt.Field(
        description="The country where the company is based",
        default=None,
        )
    sector: str = pdt.Field(
        description="The sector the company operates in",
        default=None,
        )
    industry: str = pdt.Field(
        description="The industry the company operates in",
        default=None,
        )
    fulltime_employees: int = pdt.Field(
        description="The number of full-time employees at the company",
        default=None,
        )
    business_summary: str = pdt.Field(
        description="A summary of the company's business",
        default=None,
        )
    last_updated: datetime = pdt.Field(
        description="The date the data was last updated", default=None,
        )


class Fundamentals(pa.DataFrameModel):
    """Configuration for the fundamentals.

    This configuration is used to specify the financial metrics to fetch from the Yahoo Finance API.

    """

    symbol: pl.String
    period_type: pl.String
    as_of_date: pl.Date = pa.Field(coerce=True)
    quarter: pl.Int32 = pa.Field(
        ge=1, le=4, coerce=True,
        )
    year: pl.Int32 = pa.Field(
        coerce=True,
        )
    currency_code: pl.String = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    net_income: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    cash_and_cash_equivalents: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    gross_profit: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    total_revenue: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    current_assets: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    current_liabilities: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    total_assets: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    total_liabilities_net_minority_interest: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    capital_stock: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    enterprise_value: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    enterprises_value_ebitda_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    enterprises_value_revenue_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    forward_pe_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    market_cap: pl.Int64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    pb_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    pe_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    peg_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )
    ps_ratio: pl.Float64 = pa.Field(
        default=None, coerce=True, nullable=True,
        )

class Scores(pa.DataFrameModel):
    """Score measures for the financial metrics."""

    fundamentals_id: pl.Int64 = pa.Field(
        description="The fundamentals ID",
        nullable=False, coerce=True, unique=True,
        )
    symbol: pl.String = pa.Field(
        description="The stock symbol",
        nullable=False,
        )
    roa: pl.Float64 = pa.Field(
        description="Return on assets",
        default=None, nullable=True, coerce=True,
        )
    delta_cash: pl.Float64 = pa.Field(
        description="Change in cash and cash equivalents",
        default=None, nullable=True, coerce=True,
        )
    delta_roa: pl.Float64 = pa.Field(
        description="Change in return on assets",
        default=None, nullable=True, coerce=True,
        )
    accruals: pl.Float64 = pa.Field(
        description="Accruals",
        default=None, nullable=True, coerce=True,
        )
    delta_long_lev_ratio: pl.Float64 = pa.Field(
        description="Change in long term leverage ratio",
        default=None, nullable=True, coerce=True,
        )
    delta_current_lev_ratio: pl.Float64 = pa.Field(
        description="Change in current leverage ratio",
        default=None, nullable=True, coerce=True,
        )
    delta_shares: pl.Float64 = pa.Field(
        description="Change in shares",
        default=None, nullable=True, coerce=True,
        )
    delta_gross_margin: pl.Float64 = pa.Field(
        description="Change in gross margin",
        default=None, nullable=True, coerce=True,
        )
    delta_asset_turnover: pl.Float64 = pa.Field(
        description="Change in asset turnover",
        default=None, nullable=True, coerce=True,
        )
    cash_ratio: pl.Float64 = pa.Field(
        description="Cash ratio",
        default=None, nullable=True, coerce=True,
        )
    eps: pl.Float64 = pa.Field(
        description="Earnings per share",
        default=None, nullable=True, coerce=True,
        )
    book_value: pl.Int64 = pa.Field(
        description="Book value",
        default=None, nullable=True, coerce=True,
        )
    pf_score: pl.Int32 = pa.Field(
        description="Piotroski F-Score",
        default=None, nullable=True, coerce=True,
        )
    pf_score_weighted: pl.Float64 = pa.Field(
        description="Weighted Piotroski F-Score",
        default=None, nullable=True, coerce=True,
        )


class SecurityPrices(pa.DataFrameModel):
    """Configuration for the security prices.

    This configuration is used to specify the stock prices to fetch from the Yahoo Finance API.

    """

    symbol: pl.String = pa.Field(
        description="The stock symbol",
        nullable=False,
        )
    date: pl.Date = pa.Field(
        description="The date of the stock price",
        nullable=False,
        )
    open: pl.Float64 = pa.Field(
        description="The opening price of the stock",
        nullable=True, coerce=True,
        )
    high: pl.Float64 = pa.Field(
        description="The highest price of the stock",
        nullable=True, coerce=True,
        )
    low: pl.Float64 = pa.Field(
        description="The lowest price of the stock",
        nullable=True, coerce=True,
        )
    close: pl.Float64 = pa.Field(
        description="The closing price of the stock",
        nullable=True, coerce=True,
        )
    adjclose: pl.Float64 = pa.Field(
        description="The adjusted closing price of the stock",
        nullable=True, coerce=True,
        )
    volume: pl.Int64 = pa.Field(
        description="The volume of the stock",
        nullable=True, coerce=True,
        )
    dividends: pl.Float64 = pa.Field(
        description="The dividends of the stock",
        nullable=True, coerce=True,
        )
    splits: pl.Int64 = pa.Field(
        description="The split of the stock",
        nullable=True, coerce=True,
        )

class ExpectedReturns(pa.DataFrameModel):
    """Configuration for the expected returns.

    This configuration is used to specify the expected returns for the stock prices.

    """

    fundamentals_id: pl.Int32 = pa.Field(
        description="The fundamentals ID",
        nullable=False, coerce=True, unique=True,
        )
    last_close: pl.Float64 = pa.Field(
        description="The last closing price of the stock",
        nullable=False, coerce=True,
        )
    forecasted_close: pl.Float64 = pa.Field(
        description="The forecasted closing price of the stock",
        nullable=False, coerce=True,
        )
    expected_return: pl.Float64 = pa.Field(
        description="The expected return of the stock",
        nullable=False, coerce=True,
        )
    variance: pl.Float64 = pa.Field(
        description="The variance of the stock",
        nullable=True, coerce=True,
        )
    symbol: pl.String = pa.Field(
        description="The stock symbol",
        nullable=False,
        )
