# webframe/views.py


from django.urls import reverse_lazy
from django.http import HttpResponseRedirect
from django.db.models import Max, Subquery, OuterRef
from django.db import connection
from django.conf import settings

from django.views.generic.edit import FormView
from django.views.generic.base import TemplateView
from django.shortcuts import render
# from rest_framework import viewsets

from .forms import OptimizeForm, AddDataForm
from .models import Scores, DataSettings, Portfolio, SecurityList
from .optimizer import utils, optimization, download, plots
# from .serializers import DataSettingsSerializer

import datetime
import pandas as pd
import markdown as md
import json
import random
import re
from pathlib import Path

# Create your views here.
# def index(request):
#     return render(request, "optimizer/index.html")


class IndexView(TemplateView):
    template_name = 'optimizer/index.html'
    template_path = Path(__file__).resolve().parent / 'templates/optimizer/index.md'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        with open(self.template_path, 'r') as f:
            text = f.read()
            context["about"] = md.markdown(text)
            
        return context
    
# uvicorn config.asgi:application --reload

class DashboardView(FormView):
    model = Scores
    form_class = OptimizeForm
    template_name = 'optimizer/dashboard.html'
    success_url = reverse_lazy('dashboard')
    
    # Hacky, but need this to avoid makemigration no such table error
    if 'portfolio_optimizer_datasettings' in connection.introspection.table_names():
        if not DataSettings.objects.exists():
            # Initialize empty database with defaults
            saved_initials = DataSettings.objects.create()
        else:
            saved_initials = DataSettings.objects.get(id=1)

    def get_initial(self):
        initial = super().get_initial()
        
        assert self.form_class is not None
        
        for setting in self.form_class.Meta.fields:
            initial[setting] = self.request.POST.get(setting, getattr(self.saved_initials, setting))

        return initial

    def form_valid(self, form):
        investment_amount = form.cleaned_data['investment_amount']
        objective = form.cleaned_data['objective']
        threshold = form.cleaned_data['FScore_threshold']
        method = form.cleaned_data['estimation_method']
        gamma = form.cleaned_data['l2_gamma']
        risk_aversion = form.cleaned_data['risk_aversion']

        assert self.form_class is not None

        for setting in self.form_class.Meta.fields:
            setattr(self.saved_initials, setting, form.cleaned_data[setting])
        self.saved_initials.save()

        optimalPortfolio = optimization.OptimizePorfolio(
            investment_amount=investment_amount,
            objective=objective,
            threshold=threshold,
            method=method,
            l2_gamma=gamma,
            risk_aversion=risk_aversion
        )
        optimalPortfolio.save_portfolio()

        return HttpResponseRedirect(reverse_lazy('portfolio-optimizer-dashboard'))

    def get_context_data(self, **kwargs):
        context = super(DashboardView, self).get_context_data(**kwargs)
        context['data_settings'] = DataSettings.objects.all()
        context['plots'] = {}

        # Get scores + symbol
        related_fields = ['security__symbol',
                          # 'security__longname',
                          'security__business_summary',
                          'security__portfolio__shares',
                          'security__portfolio__allocation']

        scores_fields = [field.name for field in Scores._meta.get_fields()]
        scores_fields += related_fields

        # Only most recent
        scores = Scores.objects.values(*scores_fields)

        if scores.exists():
            # context['plots'] = plots.create_plots()

            context['plots']['spx'] = plots.compare_ytd()

            # Round decimals
            field_dat = Scores._meta.get_fields() + Portfolio._meta.get_fields()
            decimal_fields = [x.name for x in field_dat if x.get_internal_type() == 'DecimalField']

            # Formatting
            df_scores = pd.DataFrame(scores)
            df_scores = df_scores.loc[df_scores.groupby(["security"])["fiscal_year"].idxmax()]

            df_scores = df_scores.astype({x: float for x in decimal_fields if x in df_scores.columns})
            df_scores = df_scores.rename(columns={x: x.split('__')[-1] for x in related_fields})
            df_scores = df_scores.sort_values(['allocation', 'symbol', 'date', 'pf_score'],
                                              ascending=False).reset_index(drop=True)

            
            df_scores.allocation = round(100 * df_scores.allocation.astype(float), 2).astype(str) + "%"
            df_scores = df_scores.round({x: 3 for x in decimal_fields})
            df_scores.cash = '$' + (df_scores.cash / 1e6).astype(str) + 'm'
            df_scores['date'] = [x.strftime("%Y-%m-%d") for x in df_scores['date']]
            df_scores.index += 1

            # parsing the DataFrame in json format.
            json_records = df_scores.reset_index().to_json(orient='records')
            data = list(json.loads(json_records))

            context['score_table'] = data

        return context

class AddDataView(FormView):
    model = Scores
    form_class = AddDataForm
    template_name = 'optimizer/add-data.html'
    success_url = reverse_lazy('portfolio-optimizer-add-data')
    snp_list = utils.get_latest_snp()
    snp_tickers = [x['Symbol'] for x in snp_list]

    def form_valid(self, form):
        if not DataSettings.objects.exists() or not self.request.user.is_authenticated:
            return HttpResponseRedirect(reverse_lazy('portfolio-optimizer-add-data'))

        symbol_fieldval = form.cleaned_data['symbols']

        # If the all symbol * is given
        if symbol_fieldval == ['*']:
            symbols = self.snp_tickers
        else:
            symbols = []
            # Check for random arg
            for symb in symbol_fieldval:
                # If random, sample N and add to symbols list
                if 'random' in symb:
                    # Extract N
                    n = re.findall(r'\d+', symb)
                    n = int(n[0]) if isinstance(n, list) and len(n) > 0 else 10

                    # Tickers to sample from, not already selected
                    remaining = [x for x in self.snp_tickers if x not in symbols]

                    # Sample and append to list
                    symbols.extend(random.sample(remaining, n))
                else:
                    symbols.append(symb)

            # Check if symbol is valid SP500
            symbols = [x for x in symbols if x in self.snp_tickers]

        # Get data
        # download.DownloadCompanyData(symbols)
        for chunk in utils.chunked_iterable(symbols, 100):
            print('Updating chunk ' + ', '.join(chunk))
            download.DownloadCompanyData(chunk)

        return HttpResponseRedirect(reverse_lazy('portfolio-optimizer-add-data'))

    def get_context_data(self, **kwargs):
        context = super(AddDataView, self).get_context_data(**kwargs)

        # Get list of snp data
        df_tickers = SecurityList.objects.filter(symbol__in=self.snp_tickers)
        df_tickers = df_tickers.values('symbol', 'last_updated', 'first_created')
        df_tickers = pd.DataFrame(df_tickers)

        df_snp = pd.DataFrame(self.snp_list)
        df_snp.columns = df_snp.columns.str.lower()

        # Add last_updated cols from database
        if df_tickers.empty:
            df_snp['start_date'] = None
            snp_data = df_snp
        else:
            # df_tickers.start_date.dt.strftime('%m/%d/%Y')
            snp_data = df_snp.merge(df_tickers, on='symbol', how='left')
        snp_data = snp_data.astype(object).where(snp_data.notna(), None)
        if 'last_updated' not in snp_data.columns:
            snp_data['last_updated'] = None

        snp_data = snp_data.sort_values(['last_updated', 'symbol'])

        # Default data settings
        if not DataSettings.objects.exists():
            data_settings = DataSettings(
                start_date=datetime.date(2010, 1, 1),
                investment_amount=10000
            )
            data_settings.save()

        context['snp_list'] = snp_data.to_dict('records')
        context['data_settings'] = DataSettings.objects.values('start_date').first()

        return context
