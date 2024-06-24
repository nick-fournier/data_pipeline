
from django.db.models import Q
from ..models import Fundamentals, SecurityPrice, SecurityList, Portfolio, Scores
from ..optimizer import utils, piotroski_fscore, optimization
import pandas as pd
import yahooquery as yq
import datetime


class DownloadCompanyData:
    def __init__(self, tickers=None, score_cutoff=7, save_to_db=True, cache=False):
        # Initialize empty dict
        self.data = {}
        self.cached_data = {}
        self.cache = cache
        self.score_cutoff = score_cutoff
        self.save_to_db = save_to_db

        # Cleanup any incomplete records before we do anything
        utils.clean_records()

        # Parameters, get out of date data
        the_tickers = [tickers] if isinstance(tickers, str) else tickers
        ood_tickers = utils.get_missing(the_tickers)

        self.end_date = datetime.date.today().strftime("%Y-%m-%d")
        self.DB_ref = {
            'fundamentals': Fundamentals,
            'securityprice': SecurityPrice,
            'scores': Scores,
            'portfolio': Portfolio
            }

        # Download
        self.download(ood_tickers)


    def download(self, ood_tickers):
        # Generate ID for new security if missing
        all_tickers = list(set([item for sublist in ood_tickers.values() for item in sublist]))
        self.id_table = utils.get_id_table(all_tickers, add_missing=True)

        # 1) Fetch company meta data
        if ood_tickers['meta']:
            meta = self.get_meta(ood_tickers['meta'])
            self.set_meta(meta)

        if ood_tickers['fundamentals']:
            # 2) First fetch all company stock fundamental data
            fundamentals = self.get_fundamentals(ood_tickers['fundamentals'])
            self.set_data(db_name='fundamentals', data=fundamentals)

            # 3) Then calculate PF score
            PFScores = piotroski_fscore.GetFscore(fundamentals)
            scores = PFScores.scores.reset_index()
            self.set_data(db_name='scores', data=scores)

            # 4) Filter out stock picks by score threshold. Keep any that ever exceed it (for backtesting)
            max_ticker_scores = (PFScores.scores.pf_score > self.score_cutoff).groupby(level=0).any()
            price_tickers = max_ticker_scores[max_ticker_scores].index

            # Add out of date prices to update, remove duplicates
            ood_tickers['securityprice'].extend(price_tickers)
            ood_tickers['securityprice'] = set(ood_tickers['securityprice'])

        if ood_tickers['securityprice']:
            # 5) Get price data only for tickers above score threshold or out of date
            securityprice = self.get_prices(ood_tickers['securityprice'])
            self.set_data(db_name='securityprice', data=securityprice)

        # 6) Ensure meta set correctly, rerun clean if not
        utils.clean_records()

        # 7) Optimize at default amount
        # optimization.OptimizePorfolio()


        print('Database update complete')

    def get_meta(self, the_tickers):
        print('Downloading stock meta data')

        fields_map = {
            'index': 'symbol',
            'country': 'country',
            'sector': 'sector',
            'industry': 'industry',
            'website': 'logo_url',
            'fullTimeEmployees': 'fulltime_employees',
            'longBusinessSummary': 'business_summary'
        }

        stock_data = yq.Ticker(the_tickers)
        meta = stock_data.asset_profile

        # Remove empty result
        meta = {k: v for k, v in meta.items() if type(v) == dict}

        meta = pd.DataFrame(meta).T.reset_index()
        meta = meta.rename(columns=fields_map)

        # self.set_meta(meta)

        return meta

    def get_fundamentals(self, the_tickers):
        print('Downloading stock fundamentals data')

        # database to scrape map
        fields_map = {
            'NetIncome': 'net_income',
            'NetIncomeCommonStockholders': 'net_income_common_stockholders',
            'TotalLiabilitiesNetMinorityInterest': 'total_liabilities',
            'TotalAssets': 'total_assets',
            'CurrentAssets': 'current_assets',
            'CurrentLiabilities': 'current_liabilities',
            'CapitalStock': 'shares_outstanding',
            'CashAndCashEquivalents': 'cash',
            'GrossProfit': 'gross_profit',
            'TotalRevenue': 'total_revenue',
         }

        stock_data = yq.Ticker(the_tickers)
        fundamentals = stock_data.get_financial_data(
            types=list(fields_map.keys()),
            frequency='a',
            trailing=False
        )

        # Add dummy data for index
        if 'Cash Flow data unavailable' in fundamentals:
            fundamentals = pd.DataFrame(None, index=['^GSPC'], columns=['asOfDate'] + list(fields_map.keys()))
            fundamentals.asOfDate = datetime.date.today()

        # Cleanup and add security id
        fundamentals = fundamentals.join(self.id_table.set_index('symbol'))
        
        fundamentals.rename(columns={**{'asOfDate': 'date'}, **fields_map}, inplace=True)
        
        if len(set(fundamentals.columns).intersection(['periodType', 'currencyCode'])) > 0:
            fundamentals.drop(columns=['periodType', 'currencyCode'], inplace=True)
            

        # Add empty fields if missing
        for f in fields_map.values():
            if f not in fundamentals.columns:
                fundamentals[f] = None

        return fundamentals

    def get_prices(self, price_tickers=None, period='5y', interval='1mo'):

        if price_tickers is None:
            return

        # Get price data if requested
        print('Downloading price data...')
        stock_data = yq.Ticker(price_tickers)
        price_data = stock_data.history(period=period, interval=interval)

        # Convert symbol to security id
        price_data = price_data.join(self.id_table.set_index('symbol')).reset_index()

        return price_data

    def set_meta(self, meta):
        # Compare current meta data to see if any are incomplete entries (i.e., new)
        incomplete_ids = [k for k, v in SecurityList.objects.values_list('symbol', 'sector') if v is None]
        incomplete_ids = list(set(meta.symbol).intersection(incomplete_ids))
        # incomplete_ids = list(set(self.the_tickers).intersection(incomplete_ids))

        if incomplete_ids:
            # Get "concrete" fields (not relation field)
            meta_fields = [f.name for f in SecurityList._meta.get_fields() if f.concrete]

            # Exclude other specific fields
            exclude = ['id', 'security', 'last_updated', 'first_created', 'has_fundamentals', 'has_securityprice']
            meta_fields = [x for x in meta_fields if x not in exclude]

            # Add Null to any missing
            missing = list(set(meta_fields).difference(meta.columns))
            meta[missing] = None

            # Subset select columns & add security id
            meta = meta[meta_fields]
            meta = meta.astype(object).where(meta.notna(), None)

            # Merge security id
            meta = meta.merge(self.id_table.rename(columns={'security_id': 'pk'}), on='symbol')

            if not meta.empty:
                # create a list of user objects that need to be updated in bulk update
                meta_bulk_update_list = []
                meta_iter = zip(meta.pk, meta[meta_fields].to_dict(orient='records'))

                for key, values_dict in meta_iter:
                    _meta = SecurityList.objects.get(id=key)

                    for field, value in values_dict.items():
                        setattr(_meta, field, value)

                    meta_bulk_update_list.append(_meta)

                # Bulk update
                SecurityList.objects.bulk_update(meta_bulk_update_list, meta_fields)

        else:
            print('Meta data already up to date')

    def set_data(self, db_name, data):

        if data.empty:
            return

        DB = self.DB_ref[db_name]
        data.columns = [x.lower().replace(' ', '_') for x in data.columns]
        data = data.replace({pd.NaT: None})

        # drop extra columns
        _fields = [field.name for field in DB._meta.get_fields()]
        _fields = list(set(data.columns).intersection(_fields)) + ['security_id']
        data = data[_fields]

        # Filter for any existing data
        Qfilter = Q(security_id__in=data['security_id']) & Q(date__in=data['date'])
        old_data = pd.DataFrame(DB.objects.filter(Qfilter).values('security_id', 'date'))

        # If old data exists, remove existing entries
        if old_data.empty:
            new_data = data
        else:
            new_data = data[
                ~(data.date.isin(old_data.date) &
                  data.security_id.isin(old_data.security_id))
            ]

        # Fix NaNs
        new_data = new_data.astype(object).where(new_data.notna(), None)
        if not new_data.empty:
            print('Updating ' + db_name + ' data...')
            DB.objects.bulk_create(DB(**vals) for vals in new_data.to_dict('records'))
            if db_name in ['fundamentals', 'scores']:
                for rec in DB.objects.filter(fiscal_year__isnull=True):
                    rec.save()

        else:
            print(db_name + ' already up to date')
