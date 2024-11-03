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
