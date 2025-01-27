#!/usr/bin/env python
"""Module contains utilities for the data pipeline."""

import datetime

exceptions: dict = {
    "symbol": "symbol",
    "periodType": "period_type",
    "currencyCode": "currency_code",
    "asOfDate": "as_of_date",
    "EnterprisesValueEBITDARatio": "enterprises_value_ebitda_ratio",
}

_exceptions = {v: k for k, v in exceptions.items()}


def quarter_dates(year, quarter):
    quarter_dates = {
        1: (
            datetime.date(year, 1, 1),
            datetime.date(year, 3, 31),
            ),
        2: (
            datetime.date(year, 4, 1),
            datetime.date(year, 6, 30),
            ),
        3: (
            datetime.date(year, 7, 1),
            datetime.date(year, 9, 30),
            ),
        4: (
            datetime.date(year, 10, 1),
            datetime.date(year, 12, 31),
            ),
    }

    return quarter_dates[quarter]


def camel_case(field: str) -> str:
    """Convert field to camel case.

    This function converts a field to camel case unless specified in the exceptions dictionary.

    Returns
    -------
        str: The field in camel case

    """
    return _exceptions.get(
        field,
        "".join(x.capitalize() or "_" for x in field.split("_")),
        )


def snake_case(field: str) -> str:
    """Convert field to snake case.

    This function converts a field to snake case.

    Returns
    -------
        str: The field in snake case

    """
    return exceptions.get(
        field,
        "".join(["_" + x.lower() if x.isupper() else x for x in field]).lstrip("_"),
        )
