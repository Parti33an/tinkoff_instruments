TOKEN = 't.ksnSX1fn0O6hzv5A90Bd06gPo57WfHZp9rAJSwTvfebR_bD30XZG9zRJ8MgmIkg1kmSeBl-0M_o-sfsJ4MgGDg'  # новый

#TOKEN = 't.BH2PMJZFJ9f-KqFCHs3Aw7N55gmbYp7ozog644OEOBHLW_Tl0BXnSmImGDYgg99swrh61cKKduxGL8b3PyWK1g' # Брокерский счет

import  tinkoff.invest as ti
import time
from tinkoff.invest.constants import INVEST_GRPC_API

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import numpy as np
from pandas import DataFrame
import warnings
warnings.filterwarnings('ignore')


ACCOUNTS = []
RUB = 'rub'

from tinkoff.invest.services import InstrumentsService, MarketDataService, InstrumentIdType
from tinkoff.invest.exceptions import RequestError

from tinkoff.invest import (
    AccessLevel,
    AccountStatus,
    CandleInstrument,
    Client,
    AsyncClient,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
    GenerateBrokerReportRequest,
    GetBrokerReportRequest,
    OperationsResponse,
    Operation,
    OperationType,
    OperationState
)

class Account:


    def __init__(self, client, account):
        self.client = client
        self.usdrur = None
        self.account_id = account.id
        self.name = account.name
        self.rub = 0
        self.opened_date = account.opened_date
        self.closed_date = account.closed_date
        self.status = account.status
        self.instruments = dict()
        self.currency = dict()
        self.data = self._get_operations_df()
        #self.instruments = self.data['figi'].unique()
    
    def get_usdrur(self):
        """
        Получаю курс только если он нужен
        :return:
        """
        if not self.usdrur:
            # т.к. есть валютные активы (у меня etf), то нужно их отконвертить в рубли
            # я работаю только в долл, вам возможно будут нужны и др валюты
            u = self.client.market_data.get_last_prices(figi=['USD000UTSTOM'])
            self.usdrur = self._cast_money(u.last_prices[0].price)

        return self.usdrur
        
    def _operation_todict(self, o : Operation):
        """
        Преобразую PortfolioPosition в dict
        :param p:
        :return:
        """
        ins = self.currency.get(o.figi) or self.instruments.get(o.figi)
        ticker = ins['ticker']
        name = ins['name'] 

        r = {
            'date': o.date,
            'type': o.type,
            'otype': o.operation_type,
            'currency': o.currency,
            'instrument_type': o.instrument_type,
            'figi': o.figi,
            'ticker':ticker,
            'name' : name,
            'quantity': o.quantity,
            #'state': o.state,
            'payment': self._cast_money(o.payment, False),
            'price': self._cast_money(o.price, False),
        }
        return r
    
    def _cast_money(self, v, to_rub=True):
        """
        https://tinkoff.github.io/investAPI/faq_custom_types/
        :param to_rub:
        :param v:
        :return:
        """
        r = v.units + v.nano / 1e9
        if to_rub and hasattr(v, 'currency') and getattr(v, 'currency') == 'usd':
            r *= self.get_usdrur()

        return r

    def _get_operations_df(self) -> Optional[DataFrame]:
        """
        Преобразую PortfolioResponse в pandas.DataFrame
        :param account_id:
        :return:
        """
        data=[]
        instruments: InstrumentsService = self.client.instruments
        
        r: OperationsResponse = self.client.operations.get_operations(
            account_id=self.account_id,
            from_= self.opened_date,
            to=datetime.utcnow()
        )
        
        if len(r.operations) < 1: return None
        
        for p in r.operations:
            ins=None
            if p.state == OperationState.OPERATION_STATE_EXECUTED:
                
                if self.currency.get(p.figi) or self.instruments.get(p.figi):
                    pass
                else:
                    if(p.figi):
                        ins = instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id = p.figi).instrument
                    ticker = getattr(ins, 'ticker' , 'RUB')
                    name = getattr(ins, 'name' , 'Рубль')
                    
                    if p.instrument_type == 'currency':
                        self.currency[p.figi]={'ticker':ticker, 'name':name, 'instrument_type': p.instrument_type, 'currency': p.currency}
                    else:
                        self.instruments[p.figi]={'ticker':ticker, 'name':name, 'instrument_type': p.instrument_type, 'currency': p.currency}
                
                record = self._operation_todict(p)
                data.append(record)
                #df = pd.DataFrame([self._operation_todict(p) for p in r.operations if p.state == OperationState.OPERATION_STATE_EXECUTED])
        df = pd.DataFrame(data)
        # https://www.datasciencelearner.com/numpy-datetime64-to-datetime-implementation/
        df["date"]=pd.to_datetime(df.date).dt.tz_localize(None)
        return df
    
    def get_money(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        
        #https://dev-gang.ru/article/sravnenie-daty-i-vremeni-v-pythons-czasovymi-pojasami-i-bez-nih-wkbsv8ew17/
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) 
                         & ((self.data['otype']==OperationType.OPERATION_TYPE_INPUT) | (self.data['otype']==OperationType.OPERATION_TYPE_OUTPUT))][['date','payment']]
    
    def get_comissions_rub(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) &
                         ((self.data['otype']==OperationType.OPERATION_TYPE_BROKER_FEE) |
                         (self.data['otype']==OperationType.OPERATION_TYPE_SERVICE_FEE))
                          & (self.data['currency']==RUB)][['date','payment']]

    def get_margin_fee(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) 
                         & (self.data['otype']==OperationType.OPERATION_TYPE_MARGIN_FEE) ][['date','payment']]

    def get_taxes(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) 
                         & ((self.data['otype']==OperationType.OPERATION_TYPE_BENEFIT_TAX) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_BENEFIT_TAX_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_BOND_TAX) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_BOND_TAX_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_DIVIDEND_TAX) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_DIVIDEND_TAX_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO_HOLD) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO_PROGRESSIVE ) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO_HOLD_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO_REFUND) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_REPO_REFUND_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_PROGRESSIVE) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_CORRECTION) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_CORRECTION_COUPON) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_TAX_CORRECTION_PROGRESSIVE) 
                              )][['date','payment']]

    def get_varmargin(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) 
                         & ((self.data['otype']==OperationType.OPERATION_TYPE_ACCRUING_VARMARGIN) |
                            (self.data['otype']==OperationType.OPERATION_TYPE_WRITING_OFF_VARMARGIN)
                          )][['date','payment']]
    
    def get_dividends(self, from_: datetime = None, to_ : datetime = datetime.utcnow() ):
        if (from_ == None):
            from_ = self.opened_date
        to_ = np.datetime64(to_)
        from_ = np.datetime64(from_)
        return self.data[(self.data['date'] >= from_) & (self.data['date'] < to_) 
                         & ((self.data['otype']==OperationType.OPERATION_TYPE_DIVIDEND) )][['date','payment']]
   
    def get_instrument_by_figi(self, figi , to_ : datetime = datetime.utcnow() ):
        to_ = np.datetime64(to_)
        buy = self.data[(self.data['figi']==figi) & (self.data['date'] < to_) & (self.data['otype']==OperationType.OPERATION_TYPE_BUY)]['quantity'].sum()
        sell = self.data[(self.data['figi']==figi) & (self.data['date'] < to_) & (self.data['otype']==OperationType.OPERATION_TYPE_SELL)]['quantity'].sum()
        return buy - sell
    
    def get_currency_by_figi(self, figi , to_ : datetime = datetime.utcnow()):
        to_ = np.datetime64(to_)
        currency = self.currency[figi]['ticker'][:3].lower()
        return self.get_instrument_by_figi(figi , to_ =to_ ) + self.data[self.data.currency == currency]['payment'].sum()
    
    def get_portfel(self, to_ : datetime = datetime.utcnow() ):
        portfel = []
        for figi in self.instruments:
            amount = self.get_instrument_by_figi(figi , to_ = to_ )
            if amount!=0:
                r = {
                    'instrument_type': self.instruments[figi] ['instrument_type'],
                    'figi': figi,
                    'ticker':self.instruments[figi] ['ticker'],
                    'currency': self.instruments[figi] ['currency'],
                    'name' : self.instruments[figi] ['name'],
                    'quantity': amount
                    }
                portfel.append(r)

        for figi in self.currency:
            amount = self.get_currency_by_figi(figi , to_ = to_ ) # + self.data[(self.data.currency == self.currency['currency'])]['payment'].sum()
            if amount!=0:
                r = {
                    'instrument_type': self.currency[figi] ['instrument_type'],
                    'figi': figi,
                    'ticker':self.currency[figi] ['ticker'],
                    'currency': self.currency[figi] ['currency'],
                    'name' : self.currency[figi] ['name'],
                    'quantity': amount
                    }
                portfel.append(r)
        
        # добавляем рублевую позицию
        mask = np.where(((self.data.otype == OperationType.OPERATION_TYPE_BUY) | (self.data.otype == OperationType.OPERATION_TYPE_SELL)) & (self.data.instrument_type != 'futures')  
                            & (self.data.currency == RUB ), True, False)
        r = {
            'instrument_type': '', #self.instruments[''] ['instrument_type'],
            'figi': '',
            'ticker':'', #self.instruments[''] ['ticker'],
            'currency':'',
            'name' : 'RUB', #self.instruments[''] ['name'],
            'quantity': self.get_money(to_ = to_)['payment'].sum() 
                        + self.get_comissions_rub(to_ = to_)['payment'].sum() 
                        + self.get_margin_fee(to_ = to_)['payment'].sum() 
                        + self.get_taxes(to_ = to_)['payment'].sum()
                        + self.get_varmargin(to_ = to_)['payment'].sum()
                        + self.get_dividends(to_ = to_)['payment'].sum()
                        + self.data[mask]['payment'].sum()
            }
        portfel.append(r)
        return pd.DataFrame(portfel)
            


with ti.Client(TOKEN, target=INVEST_GRPC_API) as client:
        counts = client.users.get_accounts().accounts
        for count in counts:
            if not (count.access_level == ti.AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS):
                print(f"Счет {count.name} доступен")
                #print(count)
                ACCOUNTS.append(Account(client, count))
            else:
                print(f"Счет {count.name} недоступен!")

report = ACCOUNTS[0].data
print("Ввод денег: ", ACCOUNTS[0].get_money()['payment'].sum())
print("Коммисий брокера: ", ACCOUNTS[0].get_comissions_rub()['payment'].sum())
print("Плата за перенос позиций: ", ACCOUNTS[0].get_margin_fee()['payment'].sum())
print("Налог:", ACCOUNTS[0].get_taxes()['payment'].sum())
print("Доход по марже:", ACCOUNTS[0].get_varmargin()['payment'].sum())
print("Дивидендный доход:", ACCOUNTS[0].get_dividends()['payment'].sum())

#print(report.head(5))
print(ACCOUNTS[0].instruments)
print(ACCOUNTS[0].currency)
print(ACCOUNTS[0].get_portfel())