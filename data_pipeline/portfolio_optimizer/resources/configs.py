#!/usr/bin/env python
"""Module contains resources and configurations for the data pipeline."""

from dagster import Config


class Params(Config):
    """Configuration for the security screen.

    This configuration is used to specify the criteria for screening stock symbols.

    """

    MAX_SYMBOL_LENGTH: int = 5
    MIN_MARKET_CAP: int = 0
    MIN_VOLUME: int = 0
    MAX_FAILURES: int = 5
    CHUNK_SIZE: int = 500
    PRICE_PERIOD: str = '5y'
    PRICE_INTERVAL: str = '1mo'
    FSCORE_CUTOFF: int = 7

# class ForecastModel(Config):
#         x_cols = [
#             'roa', 'cash_ratio', 'delta_cash', 'delta_roa', 'accruals', 'delta_long_lev_ratio',
#             'delta_current_lev_ratio', 'delta_shares', 'delta_gross_margin', 'delta_asset_turnover'
#             ]

