#!/usr/bin/env python
"""Module contains utilities for the data pipeline."""

exceptions: dict = {
    "symbol": "symbol",
    "periodType": "period_type",
    "currencyCode": "currency_code",
    "asOfDate": "as_of_date",
}
_exceptions = {v: k for k, v in exceptions.items()}

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
