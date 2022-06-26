import os
import sys
sys.path.append('..')

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, DateTime, Numeric, BigInteger
from sqlalchemy import insert

from itertools import product
import yfinance as yf

from litedata import *

import logging
currenttime = datetime.now().strftime('%Y%m%d')
logfile = os.path.join(logpath, f'liteupdate_{currenttime}.txt')
logging.basicConfig(filename=logfile, level=logging.INFO,  format='%(asctime)s - %(levelname)s - %(message)s')

class Litedata():

    def __init__(self, vendor):
        self.vendor = vendor
        self.data_path = self.get_datapath()
        self.engine = create_engine('sqlite:///%s'%(os.path.join(basepath, 'global_data.db')))
        self.symboldict = self.get_symboldict_db()
        self.symbollist = self.symboldict.keys()
        ohlcv_columns = ['%s%s'%(style, field) for style, field in product(['', 'adj_'], ['op', 'hi', 'lo', 'cl', 'vol'])]
        self.df_columns = ['date'] + ohlcv_columns + ['div', 'split']

    def get_datapath(self):
        """Obtain the backup data folder."""
        datapath = os.path.join(basepath, 'EOD_'+self.vendor)
        if not os.path.exists(datapath):
            os.mkdir(datapath)
        return datapath
    
    def get_symboldict_csv(self, list_name, closing_hour=(16, 0)):
        """Load a symbol dictionary from CSV."""
        df_symbol = pd.read_csv(os.path.join(self.data_path, 'lite_%s.csv'%(list_name)), 
                                header=0, index_col='symbol')
        tuple_symbol = list(df_symbol.itertuples())
        symboldict = {item[0]: (item[1], item[2], item[3], item[4], item[5], item[6], 
                      datetime.strptime(item[7], '%Y-%m-%d').replace(hour=closing_hour[0], minute=closing_hour[1]), 
                      datetime.strptime(item[8], '%Y-%m-%d').replace(hour=closing_hour[0], minute=closing_hour[1])) 
                      for item in tuple_symbol}
        return symboldict
    
    def create_symboltable(self):
        """Initialize an empty database with the symbol dictionary table."""
        with self.engine.connect() as con:
            try:
                stmt_select = "SELECT symbol FROM `symbol`"
                result = con.execute(stmt_select).fetchall()
            except:
                logging.info('The `symbol` table does not exist!')
                meta = MetaData()
                table_symbol = Table('symbol', meta,
                                     Column('id', Integer(), primary_key=True),  # Set primary key
                                     Column('vendor', String(8), nullable=False),
                                     Column('ex_code', String(8), nullable=False),
                                     Column('symbol', String(16), nullable=False),
                                     Column('sectype', String(16), nullable=False),
                                     Column('currency', String(8), nullable=True),
                                     Column('sectype', String(16), nullable=False),
                                     Column('name', String(255), nullable=True),
                                     Column('sector', String(255), nullable=True),
                                     Column('industry', String(255), nullable=True),
                                     Column('start_date', DateTime(), nullable=True),
                                     Column('end_date', DateTime(), nullable=True),
                                     Column('created_date', DateTime(), nullable=False),
                                     Column('updated_date', DateTime(), nullable=False))
                meta.create_all(self.engine)
    
               
    def get_symboldict_db(self):
        """Load the symbol table from database to extract the symbol dictionary."""
        self.create_symboltable()
        symboldict = dict()
        try:
            with self.engine.connect() as con:
                stmt_select = "SELECT symbol, ex_code, sectype, currency, \
                               name, sector, industry, start_date, end_date\
                               FROM `symbol` WHERE `vendor` = '%s' \
                               ORDER BY `sectype`, `start_date`, `symbol`" %(self.vendor)
                result = con.execute(stmt_select).fetchall()
                symboldict = {item[0]: (item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8])
                                        for item in result}
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            logging.info(template.format(type(ex).__name__, ex.args), '\n')

        return symboldict

    def insert_from_symboldict(self, symboldict):
        """
        Update database when new symbols in a symbol dictionary are inserted.
        """
        # Enumerate all distinct sectypes (usually only 1) and create CSV target folders if non-existent.
        setsectype = set([spec[1] for symbol, spec in symboldict.items()])
        for sectype in setsectype:
            raw_path = os.path.join(self.data_path, '%s_raw'%(sectype))
            adj_path = os.path.join(self.data_path, '%s_adj'%(sectype))
            for path in [raw_path, adj_path]:
                if not os.path.exists(path):
                    os.makedirs(path)
        # Update `symbol` table
        try:
            with self.engine.connect() as con:
                meta = MetaData()
                symboltable = Table('symbol', meta, autoload=True, autoload_with=self.engine)
                difflist = list(set(symboldict.keys()) - set(self.symbollist))
                insertlist = []
                for symbol in difflist:
                    spec = symboldict[symbol]
                    insertlist.append({'vendor': self.vendor, 'ex_code': spec[0], 'symbol': symbol, 
                                       'sectype': spec[1], 'currency': spec[2], 
                                       'name': spec[3], 'sector': spec[4], 'industry': spec[5],
                                       'start_date': spec[6], 'end_date': spec[7],
                                       'created_date': datetime.now(), 'updated_date': datetime.now()})
                insertquery = insert(symboltable)
                con.execute(insertquery, insertlist)
                logging.info('Successfully insert the symbols %s'%(difflist))
            # Create new data table for new symbols
                for symbol in difflist:
                    spec = symboldict[symbol]
                    sectype = spec[1]
                    if not self.engine.dialect.has_table(con, '%s_%s'%(sectype, symbol)):
                        table_data = Table('%s_%s'% (sectype, symbol), meta,
                                            Column('id', Integer(), primary_key=True),  # Set primary key 
                                            Column('vendor', String(8), nullable=False), # Set data vendor
                                            Column('date', DateTime(), nullable=False), # trading day
                                            Column('op', Numeric(32,4), nullable=True), # open price
                                            Column('hi', Numeric(32,4), nullable=True), # high price
                                            Column('lo', Numeric(32,4), nullable=True), # low price
                                            Column('cl', Numeric(32,4), nullable=False), # close price
                                            Column('vol', BigInteger(), nullable=True), # number of stocks traded
                                            Column('adj_op', Numeric(32,4), nullable=True), # div-adjusted open
                                            Column('adj_hi', Numeric(32,4), nullable=True), # div-adjusted high
                                            Column('adj_lo', Numeric(32,4), nullable=True), # div-adjusted low
                                            Column('adj_cl', Numeric(32,4), nullable=True), # div-adjusted close
                                            Column('adj_vol', BigInteger(), nullable=True), # div-adjusted volume
                                            Column('div', Numeric(32,4), nullable=True), # ex-dividend amounts
                                            Column('split', Numeric(32,4), nullable=True), #stock splits / reverse splits
                                            Column('update', DateTime(), nullable=False)) #latest date of data update
                        meta.create_all(self.engine)
                        logging.info('Data table for %s_%s is created.'%(sectype, symbol))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            logging.info(template.format(type(ex).__name__, ex.args), '\n')


    def remove_symbol(self, symbol):
        """Remove both the spec data and daily data table."""      
        try:
            with self.engine.connect() as con:
                con.execute('DELETE FROM symbol WHERE vendor = "%s" and symbol = "%s"' % (self.vendor, symbol))
                con.execute('DROP TABLE `%s_%s`' % (self.symboldict[symbol][1], symbol))
                logging.info('Successfully remove symbol %s.'%(symbol))
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            logging.info(template.format(type(ex).__name__, ex.args), '\n')
            logging.info('The data for symbol %s does NOT exist!'%(symbol))              
                
    def load_dbdata(self, symbol, startdate='1990-01-01', enddate='2046-12-31'):
        """Load a symbol's data from database."""
        load_symbol = pd.DataFrame()
        sectype = self.symboldict[symbol][1]

        try:
            with self.engine.connect() as con:
                stmt_select = "SELECT date, op, hi, lo, cl, vol, adj_op, adj_hi, adj_lo, adj_cl, adj_vol, div , split\
                               FROM `%s_%s` WHERE `date` between '%s' and '%s'\
                               ORDER BY date" % (sectype, symbol, startdate, enddate)
                result = con.execute(stmt_select).fetchall()
                load_symbol = pd.DataFrame(result, columns=self.df_columns)
                load_symbol['date'] = pd.to_datetime(load_symbol['date'])
                load_symbol = load_symbol.set_index('date')
                load_symbol = load_symbol.astype('float64').round(4)
                load_symbol = load_symbol.sort_index(ascending=True)
                
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            logging.info(template.format(type(ex).__name__, ex.args), '\n')

        return load_symbol             
    
    def getdf_fromcsv(self, symbol, closing_hour=(16,0)):
        """Convert CSVs from raw and adjusted data to a dataframe containing both set data. """
        sectype = self.symboldict[symbol][1]
        adj_csvname = sectype + '_' + symbol + '.csv'
        raw_csvname = 'raw_' + sectype + '_' + symbol + '.csv'
        df_adj = pd.read_csv(os.path.join(self.data_path, sectype + '_adjusted', adj_csvname),header=0)
        df_raw = pd.read_csv(os.path.join(self.data_path, sectype + '_unadjusted', raw_csvname),header=0)
        df_adj['date'] = pd.to_datetime(df_adj['date']).apply(lambda dt: dt.replace(hour=closing_hour[0], 
                                                                                    minute=closing_hour[1]))
        df_adj = df_adj.fillna(method='ffill').set_index('date')
        df_raw['date'] = pd.to_datetime(df_raw['date']).apply(lambda dt: dt.replace(hour=closing_hour[0], 
                                                                                    minute=closing_hour[1]))
        df_raw = df_raw.fillna(method='ffill').set_index('date')
        index_common = df_adj.index.intersection(df_raw.index)
        df_concat = pd.concat([df_raw.loc[index_common], df_adj.loc[index_common]], axis=1)
        df_concat = df_concat.astype("float64").round(4)
        
        return df_concat
    
    def yfinancescrape(self, symbollist, startstr='1990-01-01', endstr='2046-12-31', 
                       vecdelta=(16, 0), numthread=16):
        """Scrape via yahoo API to obtain data for a symbollist."""
        symbolstr = ' '.join(symbollist)
        rename_dict = {'Date': 'date', 'Open': 'op', 'High': 'hi', 'Low': 'lo', 'Close': 'cl', 'Volume': 'vol',
                       'Adj Close': 'adj_cl', 'Dividends': 'div', 'Stock Splits': 'split'}       
        ohlcv_columns = ['op', 'hi', 'lo', 'cl', 'vol']
        ohlcv_allcols = ['%s%s'%(style, field) for style, field in product(['', 'adj_'], ohlcv_columns)] + ['div', 'split']
        datadict = dict()
    
        try:
            dfdata = yf.download(symbolstr, start=startstr, end=endstr, auto_adjust=False, actions=True,
                                 group_by='Tickers', threads=numthread)
        except:
            logging.info('Fail to prepare the raw data from API.')
            
        for symbol in symbollist:
            try:
                df_symbol = dfdata[(symbol, )].dropna()  # Raw data for the symbol
                df_symbol = df_symbol[(df_symbol['Volume'] > 0) | (df_symbol['High'] > df_symbol['Low'])] # Filter bad data
                df_symbol = df_symbol.reset_index()
                df_symbol['Date'] = [date + timedelta(hours=vecdelta[0], minutes=vecdelta[1]) for date in df_symbol['Date']]
                df_symbol = df_symbol.rename(columns=rename_dict)
                df_symbol = df_symbol.set_index('date')
                adj_factor = df_symbol['adj_cl'] / df_symbol['cl']
                for field in ['op', 'hi', 'lo']:
                    df_symbol['adj_%s'%(field)] = df_symbol[field] * adj_factor
                df_symbol['adj_vol'] = df_symbol['vol'] / adj_factor
                df_symbol = np.round(df_symbol, 4)
                df_symbol = df_symbol[ohlcv_allcols]
                datadict[symbol] = df_symbol
            except:
                logging.info('Fail to prepare the raw data for symbol %s.'%(symbol))
       
        return datadict
              
    def update_from_yahoodf(self, symbol, df):
        """Update data of symbol from dataframe with both raw and adjusted OHLCV series."""
        # Load data from database
        load_symbol = self.load_dbdata(symbol)
        sectype = self.symboldict[symbol][1]
        if df.shape[0] == 0:
            logging.info('Input price data of symbol %s is empty!'%(symbol))
        else:
            common_index = load_symbol.index.intersection(df.index)
            if len(common_index) > 0:
                common_index_lastdate = common_index[-1]
                condition2 =(np.absolute(df.loc[common_index_lastdate,'cl']
                                - load_symbol.loc[common_index_lastdate,'cl'])>0.0001)
            else:
                condition2 = True
            # Setup dividend series and compare with database
            index_diff = df.index.difference(load_symbol.index)
            diff_symbol = df.loc[index_diff]
            diff_symbol_div = diff_symbol[np.absolute(diff_symbol['div']) > 0.0001]
            condition1 = (diff_symbol_div.shape[0] > 0)
            condition = condition1 + condition2
            
            try:          
                with self.engine.connect() as con:
                    meta = MetaData() 
                    # Construct list of data rows to be inserted 
                    if condition > 0:
                        con.execute('DELETE FROM `%s_%s`'% (sectype, symbol))
                        list_datarows = list(df.itertuples(index=True))
                    else:
                        list_datarows = list(diff_symbol.itertuples(index=True))
                    insertlist = [{'vendor': self.vendor, 'date': item[0],
                                    'op': item[1], 'hi': item[2], 'lo': item[3], 'cl': item[4], 'vol': item[5],
                                    'adj_op': item[6], 'adj_hi': item[7], 'adj_lo': item[8], 
                                    'adj_cl': item[9], 'adj_vol': item[10],
                                    'div': item[11], 'split': item[12],
                                    'update': datetime.now()} for item in list_datarows] 
                    # Execute insert
                    if len(insertlist) > 0:
                        table_ohlc = Table('%s_%s'%(sectype, symbol), meta, autoload=True, autoload_with=self.engine)
                        insertquery = insert(table_ohlc)
                        con.execute(insertquery, insertlist)
                    # Update `symbol` master table
                    df_whole = self.load_dbdata(symbol)
                    start_date = df_whole.index[0]
                    end_date = df_whole.index[-1]
                    stmt_update = 'UPDATE symbol SET start_date = "%s", end_date = "%s", \
                                   updated_date = "%s" WHERE symbol = "%s" AND vendor = "%s"'\
                                   %(start_date, end_date, datetime.now(), symbol, self.vendor)
                    con.execute(stmt_update)                
                    logging.info('Successful in updating data table for symbol %s.' %(symbol))
                    
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                logging.info(template.format(type(ex).__name__, ex.args), '\n')
                logging.info('Somewhere is wrong in updating data table for symbol %s.' %(symbol))  
                
    def updatedf_symboldict(self, symboldict, symbollist_test, vecdelta=(16,0)):
        """Update a certain sectype data."""
        ### Update a couple symbols to identify latest trading day
        list_lasttd = []
        if symbollist_test == []:
            raise AttributeError('Empty testing symbollist!')
        else:
            sectype = self.symboldict[symbollist_test[0]][1]
            datadict_test = self.yfinancescrape(symbollist_test, vecdelta=vecdelta)
            for symbol, df_symbol in datadict_test.items():
                try:
                    if self.symboldict[symbol][1] != sectype:
                        continue
                    else:
                        self.update_from_yahoodf(symbol, df_symbol)
                        lasttd_symbol = df_symbol.index[-1]
                        list_lasttd.append(lasttd_symbol)
                except:
                    logging.info('Cannot update data for symbol %s.'%(symbol))
            lasttd_sectype = max(list_lasttd)

        updatelist = []

        for symbol in symboldict.keys():
            spec = self.symboldict[symbol]
            lastdate_db = datetime.strptime(spec[-1][:19], '%Y-%m-%d %H:%M:%S')
            firstdate_db = datetime.strptime(spec[-2][:19], '%Y-%m-%d %H:%M:%S')
            condition1 = (lastdate_db < lasttd_sectype - timedelta(hours=12))
            condition2 = (firstdate_db > lasttd_sectype - timedelta(hours=180))
            if (condition1 or condition2):
                updatelist.append(symbol)
            else:
                continue
                
        datadict_update = self.yfinancescrape(updatelist, vecdelta=vecdelta)
        for symbol, df_symbol in datadict_update.items():
            try:
                self.update_from_yahoodf(symbol, df_symbol)
            except:
                logging.info('Somewhere is wrong in updating data for symbol %s.'%(symbol))
            
        return datadict_test
                       
    def export_csv_symbollist(self, sectype):
        """Export data of assets in a symbollist to CSV files."""
        df_spec = pd.read_csv(os.path.join(self.data_path, 'lite_%s.csv'%(sectype)), header=0, index_col='symbol')
        raw_path = os.path.join(self.data_path, '%s_raw'%(sectype))
        adjust_path = os.path.join(self.data_path, '%s_adj'%(sectype))

        for symbol in df_spec.index:
            try:
                df_symbol = self.load_dbdata(symbol)
                df_symbol.index = np.array([date.strftime('%Y-%m-%d') for date in df_symbol.index])
                df_raw = df_symbol[['op', 'hi', 'lo', 'cl', 'vol']]
                df_raw.to_csv(os.path.join(raw_path, 'raw_%s_%s.csv'%(sectype, symbol)), index_label='date')
                df_adjust = df_symbol[['adj_%s'%(field) for field in ['op', 'hi', 'lo', 'cl', 'vol']]]
                df_adjust.to_csv(os.path.join(adjust_path, 'adj_%s_%s.csv'%(sectype, symbol)), index_label='date')
                df_spec.loc[symbol, 'startdate'] = df_symbol.index[0]
                df_spec.loc[symbol, 'enddate'] = df_symbol.index[-1]
                logging.info('Successfully export to CSV for symbol %s.'%(symbol))
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                logging.info(template.format(type(ex).__name__, ex.args), '\n')
                logging.info('Somewhere is wrong in exporting data for symbol %s.'%(symbol))

        df_spec.to_csv(os.path.join(self.data_path, 'lite_%s.csv'%(sectype)))

logging.shutdown()