
"""
 - compare % change in close price versus PF-score of previous year
 This is to see if there's a correlation between price change and score

 - Also compare % change in close price vs % Change in PF-score
 This is to see if there's a correlation between price change and score change

"""

from ..models import SecurityList, Scores, SecurityPrice, Portfolio
from ..optimizer.piotroski_fscore import GetFscore


from django.db.models import Q, Max, OuterRef, Subquery
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.neural_network import MLPRegressor
import scipy.stats as stats
from pypfopt.efficient_frontier import EfficientFrontier
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.discrete_allocation import DiscreteAllocation
from pypfopt.objective_functions import L2_reg

def pct_change_from_first(x):
    return (x - x.iloc[0])/x.iloc[0]

def rescore(z, mean, sd):
    return z * sd + mean

def minmax(v):
    return (v - v.min()) / (v.max() - v.min())

def get_analysis_data():
    score_cols = ['security_id', 'date', 'security__symbol', 'fiscal_year',
                  'pf_score', 'pf_score_weighted', 'eps', 'pe_ratio', 'roa', 'cash', 'cash_ratio',
                  'delta_cash', 'delta_roa', 'accruals', 'delta_long_lev_ratio',
                  'delta_current_lev_ratio', 'delta_shares', 'delta_gross_margin', 'delta_asset_turnover']

    # financials = models.Fundamentals.objects.all().values('security_id', 'date')
    prices_qry = SecurityPrice.objects.all().values('security_id', 'date', 'close')
    scores_qry = Scores.objects.all().values(*score_cols)

    # As dataframe
    # financials = pd.DataFrame(financials)
    prices = pd.DataFrame(prices_qry)
    df = pd.DataFrame(scores_qry).rename(columns={'security__fundamentals__fiscal_year': 'year'})

    pd.DataFrame(Scores.objects.all().values('security_id', 'date', 'security__fundamentals__fiscal_year'))

    # update scores
    # pfobject = GetFscore()
    # pfobject.save_scores()

    # Aggregate price data annually
    prices.close = prices.close.astype(float)
    prices.date = pd.to_datetime(prices.date)

    col_names = {'date': 'year', 'last': 'yearly_close', 'var': 'variance'}
    prices_year = prices.groupby([prices.date.dt.year, 'security_id']).close\
        .agg(['last', 'mean', 'var']).reset_index()\
        .rename(columns=col_names)

    # Add year and merge prices
    df.date = pd.to_datetime(df.date)
    df.rename(columns={'fiscal_year': 'year'}, inplace=True)
    df = df.merge(prices_year, on=['security_id', 'year'])

    # df.yearly_close = df.yearly_close.astype(float)
    df.pe_ratio = df.pe_ratio.astype(float)
    df = df.sort_values(['security_id', 'date'])

    return df

class OptimizePorfolio:
    def __init__(self,
                 investment_amount=10000,
                 threshold=6,
                 objective='max_sharpe',
                 method='nn',
                 l2_gamma=2,
                 risk_aversion=1,
                 backcast=False
                 ):
        
        
        self.investment_amount = investment_amount
        self.threshold = threshold
        self.objective = objective
        self.method = method
        self.l2_gamma = l2_gamma
        self.risk_aversion = risk_aversion
        self.backcast = backcast
        
        data = get_analysis_data()
        expected_returns = self.forecast_expected_returns(data)
        self.portfolio = self.optimize(expected_returns=expected_returns)
        # self.save_portfolio()

    def forecast_expected_returns(self, company_df):

        x_cols = ['roa', 'cash_ratio', 'delta_cash', 'delta_roa', 'accruals', 'delta_long_lev_ratio',
                  'delta_current_lev_ratio', 'delta_shares', 'delta_gross_margin', 'delta_asset_turnover']

        model_df = company_df.set_index(['security_id', 'year'])[x_cols + ['yearly_close']].copy().astype(float)

        # Get lagged value of features from t-1
        model_df['lag_close'] = model_df.groupby('security_id', group_keys=False)['yearly_close'].shift(-1)

        # Normalize close price within group since that's company-level feature, all else are high level
        df_grps = model_df[~model_df.lag_close.isnull()].groupby('security_id', group_keys=False)
        model_df.loc[~model_df.lag_close.isnull(), 'norm_lag_close'] = df_grps['lag_close'].apply(stats.zscore)

        # Drop/fill NA and normalize
        model_df = model_df.dropna(subset=x_cols)
        model_df.loc[:, x_cols] = model_df[x_cols].apply(stats.zscore)

        # Store mean and std dev for later
        grp_stats = df_grps['lag_close'].agg(mean=np.mean, std=np.std)

        # temporary fy column to groupby on easily...
        model_df['fy'] = model_df.index.get_level_values('year')

        # Initalize DF to join to
        i = model_df.fy.min() + 1

        if not self.backcast:
            i = model_df.fy.max()

        while i <= model_df.fy.max():
            # Model matrix for time<i
            df_t = model_df[model_df.fy < i]
            # Drop any missing years
            df_t = df_t[~df_t.norm_lag_close.isna()]
            # Prediction matrix for time i
            df_t0 = model_df[model_df.fy == i]

            # Assemble matrices
            y = df_t['norm_lag_close']#.to_numpy()
            x = df_t[x_cols].astype(float)#.to_numpy()

            # Estimate model
            if self.method == 'nn':
                model = MLPRegressor(max_iter=1000).fit(x, y)
            else:
                model = LinearRegression().fit(x, y)
                print(pd.Series(list(model.coef_), index=x_cols))
            print(f'R^2 = {model.score(x, y)}')


            # Make predictions
            yhat = pd.DataFrame(model.predict(df_t0[x_cols]), index=df_t0.index, columns=['yhat']).join(grp_stats)
            yhat['next_close'] = yhat.apply(lambda row: rescore(row['yhat'], row['mean'], row['std']), axis=1)

            # Calculate returns
            model_df.loc[yhat.index, yhat.columns] = yhat
            i += 1

        # Expected returns as percent change
        expected_returns = model_df.apply(lambda x: (x.next_close - x.yearly_close)/x.yearly_close, axis=1)
        expected_returns.name = 'expected_returns'

        return expected_returns

    def optimize(self, expected_returns):

        # Check type
        
        investment_amount = int(self.investment_amount)

        # # Forecast expected returns
        returns_df = expected_returns[~expected_returns.isna()]

        # Remove companies below score threshold
        returns_ids = returns_df.index.get_level_values('security_id').unique()

        # Find company IDs above threshold for current year and has forecasted return data available
        sq = Scores.objects.filter(security_id=OuterRef('security_id')).order_by('-fiscal_year')
        security_ids = list(
            Scores.objects.filter(
                Q(pk=Subquery(sq.values('pk')[:1])) & Q(pf_score__gte=self.threshold) & Q(security_id__in=returns_ids)
            ).values_list('security_id', flat=True)
        )

        # Some formatting
        prices = SecurityPrice.objects.filter(security_id__in=security_ids)
        prices = pd.DataFrame(prices.values('security_id', 'date', 'close'))
        prices.date = pd.to_datetime(prices.date)
        prices['year'] = prices.date.dt.year
        prices.close = prices.close.astype(float)

        # Get price data to wide
        prices = prices.drop_duplicates()
        # prices_wide = {yr: df.pivot(index='date', columns='security_id', values='close').dropna() for yr, df in prices.groupby('year')}
        # prices_wide = prices.pivot(index='date', columns='security_id', values='close').dropna()

        weights_dict = {}
        for year, exp_df in returns_df.groupby(level='year'):

            # Get prices for available stocks, # Remove companies below score threshold
            exp_ids = exp_df.index.get_level_values('security_id').unique()
            year_ids = list(set(security_ids).intersection(exp_ids))

            # Cast prices data to wide form
            prices_wide = prices[prices.year == year]\
                .pivot(index='date', columns='security_id', values='close')\
                .dropna()

            # make data consistent
            exp_df = exp_df.loc[security_ids]
            these_prices = prices_wide[year_ids]

            # Calculate covariance matrix
            prices_cov = CovarianceShrinkage(these_prices).ledoit_wolf()

            # Optimize efficient frontier of Mean Variance
            ef = EfficientFrontier(exp_df.to_numpy(), prices_cov)
            # Reduces 0 weights
            ef.add_objective(L2_reg, gamma=self.l2_gamma)

            # Get the allocation weights
            if self.objective == 'max_sharpe':
                weights_dict[year] = ef.max_sharpe()
            elif self.objective == 'max_quadratic_utility':
                weights_dict[year] = ef.max_quadratic_utility(risk_aversion=self.risk_aversion)
            else:
                weights_dict[year] = ef.min_volatility()


        # Discrete allocation from portfolio value
        portfolio_year = max(weights_dict.keys())
        latest_weights = weights_dict[portfolio_year]
        # Only do this for current year. Everything prior can be unitless
        latest_prices = prices.loc[prices.groupby('security_id').date.idxmax()].set_index('security_id')
        
        disc_allocation, cash = DiscreteAllocation(latest_weights,
                                                   latest_prices.close,
                                                   total_portfolio_value=investment_amount,
                                                   short_ratio=None).greedy_portfolio()

        # Format into dataframe
        df_allocation = pd.concat([
            pd.Series(latest_weights, name='allocation'),
            pd.Series(disc_allocation, name='shares')
        ], axis=1)
        df_allocation.index.name = 'security_id'
        df_allocation.reset_index(inplace=True)
        df_allocation = df_allocation.fillna(0)

        # Reindex for all 0% stocks
        all_security_ids = list(SecurityList.objects.all().values_list('pk', flat=True))
        
        assert all_security_ids is not None
        df_allocation = df_allocation.set_index('security_id').reindex(all_security_ids).fillna(0).reset_index()

        # Add allocation year
        df_allocation['year'] = portfolio_year

        return df_allocation

    def save_portfolio(self):
        # Send to database
        if not self.portfolio.empty:
            if Portfolio.objects.exists():
                Portfolio.objects.all().delete()
                
            Portfolio.objects.bulk_create(
                Portfolio(**vals) for vals in self.portfolio.to_dict('records')
            )
            
        return



