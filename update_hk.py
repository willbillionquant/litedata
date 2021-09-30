# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 20:17:37 2020

@author: willbillion
"""

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append('..')

from litedata import basepath
from init_db import init_dbsetup
dbfile = os.path.join(basepath, 'global_data.db')
if not os.path.exists(dbfile):
    init_dbsetup()
    
from datetime import datetime
currenttime = datetime.now().strftime('%Y%m%d')
import logging
logfile = 'liteupdate_%s.txt'%(currenttime)
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('START Updating Asian data:')

from updatesectype_flow import update_sectypedata
from litedata import Litedata

list_hksymbol = ['0388.HK', '0700.HK', '2318.HK', '3690.HK']
list_indexsymbol = ['^DJI', '^GSPC', '^VIX', '^HSI']

if __name__ == '__main__':
    Yahoodata = Litedata('yahoo')
    update_sectypedata('Indices', list_indexsymbol, (16,0), 18, 5)
    update_sectypedata('hkstock', list_hksymbol, (16,0), 18, 5)
    
logging.info('END Updating Asian data.')
logging.shutdown()