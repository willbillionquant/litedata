import sys
sys.path.append('..')

import numpy as np
import pandas as pd

import yfinance as yf
from litedata.litedb import *

Yahoodata = Litedata('yahoo')
ohlcvdfield = ['op', 'hi', 'lo', 'cl', 'vol', 'div']


def getyahoosymbol(symbol, adjust=True, startstr='1990-01-01', endstr='2046-12-31'):
    """Scrape via yahoo API to obtain data for a single symbol."""
    renamedict = {'Date': 'date', 'Open': 'op', 'High': 'hi', 'Low': 'lo', 'Close': 'cl', 'Volume': 'vol',
                  'Adj Close': 'adj_cl', 'Dividends': 'div', 'Stock Splits': 'split'}

    try:
        dfsymbol = yf.download(symbol, start=startstr, end=endstr, auto_adjust=False, actions=True,
                               group_by='Tickers', threads=16)
        dfsymbol = dfsymbol[(dfsymbol['Volume'] > 0) | (dfsymbol['High'] > dfsymbol['Low'])]  # Filter bad data
        dfsymbol = dfsymbol.reset_index()
        dfsymbol = dfsymbol.rename(columns=renamedict)
        dfsymbol = dfsymbol.set_index('date')
        if not adjust:
            dfsymbol = dfsymbol[ohlcvdfield]
            dfsymbol = dfsymbol.rename(columns={field: f'{symbol}_{field}' for field in ohlcvdfield})
        else:
            adjfactor = dfsymbol['adj_cl'] / dfsymbol['cl']
            for field in ohlcvdfield[:-2]:
                dfsymbol[f'adj_{field}'] = dfsymbol[field] * adjfactor
            dfsymbol['adj_vol'] = dfsymbol['vol'] / adjfactor
            dfsymbol = dfsymbol[[f'adj_{field}' for field in ohlcvdfield[:-1]]]
            dfsymbol = dfsymbol.rename(columns={f'adj_{field}': f'{symbol}_{field}' for field in ohlcvdfield[:-1]})
            dfsymbol = np.round(dfsymbol, 4)
    except:
        dfsymbol = pd.DataFrame()
        print(f'Failed preparing data for {symbol}.')

    dfsymbol = np.round(dfsymbol, 4)

    return dfsymbol

def getyahoodata(symbollist, adjust=True, startstr='1990-01-01', endstr='2046-12-31'):
    """Scrape via yahoo API to obtain data for a symbollist."""
    symbolstr = ' '.join(symbollist)
    renamedict = {'Date': 'date', 'Open': 'op', 'High': 'hi', 'Low': 'lo', 'Close': 'cl', 'Volume': 'vol',
                   'Adj Close': 'adj_cl', 'Dividends': 'div', 'Stock Splits': 'split'}
    datadict = dict()

    try:
        dfdata = yf.download(symbolstr, start=startstr, end=endstr, auto_adjust=False, actions=True,
                                 group_by='Tickers', threads=16)
    except:
        dfdata = pd.DataFrame()

    for symbol in symbollist:
        try:
            dfsymbol = dfdata[(symbol, )].dropna()  # Raw data for the symbol
            dfsymbol = dfsymbol[(dfsymbol['Volume'] > 0) | (dfsymbol['High'] > dfsymbol['Low'])] # Filter bad data
            dfsymbol = dfsymbol.reset_index()
            dfsymbol = dfsymbol.rename(columns=renamedict)
            dfsymbol = dfsymbol.set_index('date')
            if not adjust:
                dfsymbol = dfsymbol[ohlcvdfield]
                dfsymbol = dfsymbol.rename(columns={field: f'{symbol}_{field}' for field in ohlcvdfield})
            else:
                adjfactor = dfsymbol['adj_cl'] / dfsymbol['cl']
                for field in ohlcvdfield[:-2]:
                    dfsymbol[f'adj_{field}'] = dfsymbol[field] * adjfactor
                dfsymbol['adj_vol'] = dfsymbol['vol'] / adjfactor
                dfsymbol = dfsymbol[[f'adj_{field}' for field in ohlcvdfield[:-1]]]
                dfsymbol = dfsymbol.rename(columns={f'adj_{field}': f'{symbol}_{field}' for field in ohlcvdfield[:-1]})
                dfsymbol = np.round(dfsymbol, 4)
            datadict[symbol] = dfsymbol
        except:
            print(f'Failed preparing data for {symbol}.')

    dfallsymbols = pd.concat(datadict.values(), axis=1)
    dfallsymbols = dfallsymbols.fillna(method='ffill')
    dfallsymbols = np.round(dfallsymbols, 4)

    return dfallsymbols

def getlitedata(symbollist, adjust=True, startstr='1990-01-01', endstr='2046-12-31'):
    """Obtain via standalone Yahoo database."""
    datadict = dict()
    ohlcvdfield = ['op', 'hi', 'lo', 'cl', 'vol', 'div']
    for symbol in symbollist:
        try:
            dfsymbol = Yahoodata.load_dbdata(symbol, startstr, endstr)
            if not adjust:
                dfsymbol = dfsymbol[ohlcvdfield]
                dfsymbol = dfsymbol.rename(columns={field: f'{symbol}_{field}' for field in ohlcvdfield})
            else:
                dfsymbol = dfsymbol[[f'adj_{field}' for field in ohlcvdfield[:-1]]]
                dfsymbol = dfsymbol.rename(columns={f'adj_{field}': f'{symbol}_{field}' for field in ohlcvdfield[:-1]})
            dfsymbol = dfsymbol.reset_index()
            dfsymbol['date'] = [date.strftime('%Y-%m-%d') for date in dfsymbol['date']]
            dfsymbol['date'] = pd.to_datetime(dfsymbol['date'])
            dfsymbol = dfsymbol.set_index('date')
            datadict[symbol] = dfsymbol
        except:
            print(f'Skip symbol {symbol}.')

    dfallsymbols = pd.concat(datadict.values(), axis=1)
    dfallsymbols = dfallsymbols.fillna(method='ffill')

    return dfallsymbols

def getyahoofutures(symbol, startstr='2010-01-01', endstr='2046-12-31', futures=True):
    """Scrape via yahoo API to obtain data for a single symbol."""
    renamedict = {'Date': 'date', 'Open': 'op', 'High': 'hi', 'Low': 'lo', 'Close': 'cl', 'Volume': 'vol',
                  'Adj Close': 'adj_cl', 'Dividends': 'div', 'Stock Splits': 'split'}
    ohlcvdfield = ['op', 'hi', 'lo', 'cl', 'vol', 'div']
    if futures:
        fsymbol = f'{symbol}=F'
    else:
        fsymbol = symbol
    try:
        dfsymbol = yf.download(fsymbol, start=startstr, end=endstr, auto_adjust=False, actions=True,
                               group_by='Tickers', threads=16)
        dfsymbol = dfsymbol[(dfsymbol['Volume'] > 0) | (dfsymbol['High'] > dfsymbol['Low'])]  # Filter bad data
        dfsymbol = dfsymbol.reset_index()
        dfsymbol = dfsymbol.rename(columns=renamedict)
        dfsymbol = dfsymbol.set_index('date')
        adjfactor = dfsymbol['adj_cl'] / dfsymbol['cl']
        for field in ohlcvdfield[:-2]:
            dfsymbol[f'adj_{field}'] = dfsymbol[field] * adjfactor
        dfsymbol['adj_vol'] = dfsymbol['vol'] / adjfactor
        dfsymbol = dfsymbol[[f'adj_{field}' for field in ohlcvdfield[:-1]]]
        dfsymbol = dfsymbol.rename(columns={f'adj_{field}': f'{symbol}_{field}' for field in ohlcvdfield[:-1]})
        dfsymbol = np.round(dfsymbol, 4)
    except:
        dfsymbol = pd.DataFrame()
        print(f'Failed preparing data for {symbol}.')

    dfsymbol = np.round(dfsymbol, 4)

    return dfsymbol