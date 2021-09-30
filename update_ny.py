# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 20:26:43 2020

@author: willbillion
"""

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append('..')

from litedata import basepath

import logging

from litedata import Litedata
from init_db import init_dbsetup
dbfile = os.path.join(basepath, 'global_data.db')
if not os.path.exists(dbfile):
    init_dbsetup()

from datetime import datetime
currenttime = datetime.now().strftime('%Y%m%d')
logfile = 'liteupdate_%s.txt'%(currenttime)
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('START Updating US data:')

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
    list_sectype = ['Indices', 'usstock', 'largecap', 'coreetf', 'altetf', 'levetf']
    for sectype in list_sectype:
        update_sectypedata(sectype, dict_testsymbol[sectype])

logging.info('END Updating US data.')
logging.shutdown()