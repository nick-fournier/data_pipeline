
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

import datetime
from enum import Enum

import polars as pl
from dagster import asset
from pypfopt.discrete_allocation import DiscreteAllocation
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.objective_functions import L2_reg
from pypfopt.risk_models import CovarianceShrinkage

from data_pipeline.portfolio_optimizer.resources.dbtools import _append_data
from data_pipeline.portfolio_optimizer.resources.models import Portfolio
from data_pipeline.portfolio_optimizer.utils import quarter_dates


class OptimizeConfigs(Enum):
    investment_amount = 10000
    threshold = 6
    objective = 'max_sharpe'
    l2_gamma = 2
    risk_aversion = 1
    backcast = False
    risk_free_rate = 0.04


@asset(
    description="Optimize portfolio allocation based on expected returns",
    io_manager_key="pgio_manager",
)
def optimize(
    context,
    expected_returns: pl.DataFrame,
    security_price: pl.DataFrame,
    fundamentals: pl.DataFrame,
    ) -> None:

    # Connection to database
    pg_config = context.resources.pgio_manager.postgres_config

    # Load existing portfolio data
    try:
        existing_portfolios = pl.read_database_uri(
            query="SELECT fundamentals_id FROM portfolio",
            uri=pg_config.db_uri(),
        )

    except RuntimeError:
        existing_portfolios = pl.DataFrame(schema={'fundamentals_id': pl.Int64()})

    # Find missing fundamentals_id in portfolio
    new_expected_returns = expected_returns.filter(
        ~pl.col('fundamentals_id').is_in(existing_portfolios['fundamentals_id'])
    ).join(
        fundamentals.select('id', 'as_of_date', 'quarter', "year"),
        left_on='fundamentals_id',
        right_on='id',
        how='inner'
    )


    # Process the efficient frontier prices up to current quarter
    grouper = new_expected_returns.sort(
        'as_of_date', descending=True,
    ).group_by(['year', 'quarter'], maintain_order=True)

    schema = {k: v.dtype.type for k, v in Portfolio.to_schema().columns.items()}
    new_portfolio = pl.DataFrame(schema=schema)

    for (year, quarter,), exp_df in grouper:

        # Quarter date range
        _, qdate_end = quarter_dates(year, quarter)

        print("Optimizing for quarter:", year, quarter)

        if not isinstance(qdate_end, datetime.date):
            raise ValueError("as_of_date must be a datetime.date object")

        # Get prices up to the last date of the quarter
        prices = security_price.filter(
            pl.col('symbol').is_in(exp_df['symbol']),
            pl.col('date') <= qdate_end,
        )

        # Pivot prices to wide form for covariance calculation
        prices_wide = prices.pivot(
            on='symbol', values='close', index='date',
        ).drop_nulls().to_pandas().set_index('date')

        # Calculate covariance matrix
        prices_cov = CovarianceShrinkage(prices_wide).ledoit_wolf()
        exp_returns = exp_df.to_pandas().set_index('symbol')["expected_return"]

        # Optimize efficient frontier of Mean Variance
        ef = EfficientFrontier(exp_returns, prices_cov)

        # Get the allocation weights
        objective = OptimizeConfigs.objective.value
        if objective == 'max_sharpe':
            weights = ef.max_sharpe(
                risk_free_rate=OptimizeConfigs.risk_free_rate.value,
            )

        elif objective == 'max_quadratic_utility':
            # Reduces 0 weights
            ef.add_objective(L2_reg, gamma=OptimizeConfigs.l2_gamma.value)
            weights = ef.max_quadratic_utility(
                risk_aversion=OptimizeConfigs.risk_aversion.value,
            )

        elif objective == 'min_volatility':
            # Reduces 0 weights
            # ef.add_objective(L2_reg, gamma=OptimizeConfigs.l2_gamma.value)
            weights = ef.min_volatility()

        else:
            raise ValueError("Invalid objective function")

        # Latest prices for each security
        latest_prices = (
            prices.sort('date')
            .group_by('symbol').last()
            .select('symbol', 'close')
            .to_pandas().set_index('symbol')['close']
        )

        # Discrete allocation from portfolio value
        allocator = DiscreteAllocation(
            weights,
            latest_prices,
            total_portfolio_value=OptimizeConfigs.investment_amount.value,
            short_ratio=None,
        )
        allocation, cash = allocator.greedy_portfolio()

        # Format into dataframe
        combined = []
        for symbol, fund_id in exp_df.select('symbol', 'fundamentals_id').iter_rows():
            row = (fund_id, symbol, allocation.get(symbol, 0), weights.get(symbol, 0))
            combined.append(row)

        _portfolio = pl.DataFrame(
            combined,
            orient='row',
            schema=["fundamentals_id", "symbol", "shares", "allocation"],
        )

        _portfolio_valid: pl.DataFrame = Portfolio.validate(_portfolio) # type: ignore

        # Extend portfolio with new allocation
        new_portfolio.extend(_portfolio_valid.select(new_portfolio.columns))

    # Append to database
    _append_data(
        uri=pg_config.db_uri(),
        table_name="portfolio",
        new_data=new_portfolio,
        pk=["fundamentals_id"],
    )
