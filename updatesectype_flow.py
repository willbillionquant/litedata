# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 20:14:39 2020

@author: willbillion
"""

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append('..')
from datetime import datetime, timedelta

import logging
currenttime = datetime.now().strftime('%Y%m%d')
logfile = 'liteupdate_%s.txt'%(currenttime)
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from litedata import Litedata
Yahoodata = Litedata('yahoo')

def update_sectypedata(sectype, symbollist_test, vecdelta=(16, 0), 
                       tzoffset=6, export_wkday=6):
    symboldict = Yahoodata.get_symboldict_csv(sectype)
    Yahoodata.updatedf_symboldict(symboldict, symbollist_test, vecdelta)
    export_date = datetime.now() - timedelta(hours=tzoffset)
    if export_date.weekday() >= export_wkday - 1:
        Yahoodata.export_csv_symbollist(sectype)
        
logging.shutdown()