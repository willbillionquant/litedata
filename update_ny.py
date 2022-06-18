import os
import sys
sys.path.append('..')
from datetime import datetime

from litedata import *
from litedata.litedb import *
import logging
currenttime = datetime.now().strftime('%Y%m%d')
logfile = f'liteupdate_{currenttime}.txt'
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('START Updating US data:')

from setupdb import init_dbsetup
dbfile = os.path.join(basepath, 'global_data.db')
if not os.path.exists(dbfile):
    init_dbsetup()

from updatesectype_flow import update_sectypedata

dict_testsymbol = {'Indices': ['^DJI', '^GSPC', '^VIX', '^HSI'],
                   'usstock': ['IOVA', 'MELI', 'SE', 'SQ'],
                   'largecap': ['AAPL', 'AMZN', 'MSFT', 'NVDA'],
                   'coreetf': ['SPY', 'QQQ', 'IWM', 'ARKK'],
                   'altetf': ['TLT', 'GLD', 'SLV', 'HYG'],
                   'levetf': ['TQQQ', 'SPXL', 'TMF', 'TECL']
                   }

if __name__ == '__main__':
    Yahoodata = Litedata('yahoo') 
    list_sectype = ['Indices', 'coreetf', 'altetf', 'levetf', 'largecap', 'usstock']
    for sectype in list_sectype:
        update_sectypedata(sectype, dict_testsymbol[sectype])

logging.info('END Updating US data.')
logging.shutdown()