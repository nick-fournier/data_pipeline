from django.core.validators import MaxValueValidator, MinValueValidator
from django.db.models import CheckConstraint, Q
from django.db import models
import pandas as pd
import datetime

# TODO Clean up the data fields to be more efficent (e.g., divide by million, as int, etc.
#  Or at least convert int to IntegerFields. May cause issue with NAs?
# TODO user-specific entries?

def get_fiscal_year(date):
    assert date
    # Add fiscal year
    the_date = pd.to_datetime(date)
    fy_dates = {x: pd.to_datetime(f"{x}-12-31") for x in range(the_date.year - 1, datetime.date.today().year)}
    # get all differences with date as values
    cloz_dict = {abs(the_date.timestamp() - fydate.timestamp()): yr for yr, fydate in fy_dates.items()}
    # extracting minimum key using min()
    result = cloz_dict[min(cloz_dict.keys())]
    return result


class DataSettings(models.Model):
    OBJ_CHOICES = [
        ('max_sharpe', 'Maximum Sharpe Ratio'),
        ('min_volatility', 'Minimum Volatility'),
        ('max_quadratic_utility', 'Maximum Quadratic Utility')
    ]
    ESTIMATION_CHOICES = [
        ('nn', 'Neural Net'),
        ('lm', 'Linear Regression')
    ]

    start_date = models.DateField(default=datetime.date(2010, 1, 1))
    investment_amount = models.FloatField(default=10000)
    FScore_threshold = models.IntegerField(default=6)
    objective = models.CharField(default='max_sharpe', choices=OBJ_CHOICES, max_length=24)
    estimation_method = models.CharField(default='max_sharpe', choices=ESTIMATION_CHOICES, max_length=16)
    l2_gamma = models.FloatField(default=2)
    risk_aversion = models.FloatField(
        default=1,
        validators=[MinValueValidator(0.01), MaxValueValidator(1.0)],
        )


class SecurityList(models.Model):
    symbol = models.CharField(max_length=12, primary_key=True)
    last_updated = models.DateTimeField(auto_now=True)
    first_created = models.DateTimeField(auto_now_add=True)
    # exchange_id = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    # currency = models.CharField(default=None, null=True, max_length=3)
    # longname = models.CharField(default=None, null=True, max_length=100)
    country = models.CharField(default=None, null=True, max_length=100)
    sector = models.CharField(default=None, null=True, max_length=50)
    industry = models.CharField(default=None, null=True, max_length=50)
    logo_url = models.CharField(default=None, null=True, max_length=100)
    fulltime_employees = models.IntegerField(default=None, null=True)
    business_summary = models.CharField(default=None, null=True, max_length=3000)
    has_fundamentals = models.BooleanField(default=False)
    has_securityprice = models.BooleanField(default=False)

    def __str__(self):
        return self.symbol

class Portfolio(models.Model):
    security = models.ForeignKey(SecurityList, on_delete=models.CASCADE)
    allocation = models.DecimalField(max_digits=10, null=True, decimal_places=6)
    shares = models.IntegerField(default=None, null=True)
    fiscal_year = models.IntegerField(default=None, null=True)

class Scores(models.Model):
    security = models.ForeignKey(SecurityList, on_delete=models.CASCADE)
    date = models.DateField(null=True)
    fiscal_year = models.IntegerField(default=None, null=True)
    pf_score = models.IntegerField(default=None, null=True)
    pf_score_weighted = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    eps = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    pe_ratio = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    roa = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    cash = models.BigIntegerField(default=None, null=True)
    cash_ratio = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_cash = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_roa = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    accruals = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_long_lev_ratio = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_current_lev_ratio = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_shares = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_gross_margin = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    delta_asset_turnover = models.DecimalField(max_digits=16, default=None, null=True, decimal_places=6)
    # yearly_close = models.DecimalField(max_digits=17, null=True, decimal_places=2)
    # yearly_variance = models.DecimalField(max_digits=17, null=True, decimal_places=2)

    def save(self, *args, **kwargs):
        self.fiscal_year = get_fiscal_year(self.date)
        super(Scores, self).save(*args, **kwargs)

class SecurityPrice(models.Model):
    security = models.ForeignKey(SecurityList, on_delete=models.CASCADE)
    date = models.DateField(null=True)
    open = models.DecimalField(max_digits=10, decimal_places=6)
    high = models.DecimalField(max_digits=10, decimal_places=6)
    low = models.DecimalField(max_digits=10, decimal_places=6)
    close = models.DecimalField(max_digits=10, decimal_places=6)
    adjclose = models.DecimalField(max_digits=10, decimal_places=6)
    # dividends = models.DecimalField(max_digits=10, decimal_places=6)
    # splits = models.IntegerField(default=None, null=True)
    volume = models.IntegerField(default=None, null=True)


class Fundamentals(models.Model):
    security = models.ForeignKey(SecurityList, on_delete=models.CASCADE)
    date = models.DateField(null=True)
    fiscal_year = models.IntegerField(default=None, null=True)
    net_income = models.IntegerField(default=None, null=True)
    net_income_common_stockholders = models.IntegerField(default=None, null=True)
    total_liabilities = models.IntegerField(default=None, null=True)
    total_assets = models.IntegerField(default=None, null=True)
    current_assets = models.IntegerField(default=None, null=True)
    current_liabilities = models.IntegerField(default=None, null=True)
    shares_outstanding = models.IntegerField(default=None, null=True)
    cash = models.IntegerField(default=None, null=True)
    gross_profit = models.IntegerField(default=None, null=True)
    total_revenue = models.IntegerField(default=None, null=True)

    def save(self, *args, **kwargs):
        self.fiscal_year = get_fiscal_year(self.date)
        super(Fundamentals, self).save(*args, **kwargs)
