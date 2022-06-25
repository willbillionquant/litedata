import os
import sys
sys.path.append('..')
from datetime import datetime, timedelta

import logging
currenttime = datetime.now().strftime('%Y%m%d')
logfile = 'liteupdate_%s.txt'%(currenttime)
logging.basicConfig(filename=logfile, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

from litedata.litedb import *
Yahoodata = Litedata('yahoo')

def update_sectypedata(sectype, symbollist_test, vecdelta=(16, 0), tzoffset=6, export_wkday=6):
    allsymboldict = Yahoodata.get_symboldict_db()
    symboldict = {symbol: spec for symbol, spec in allsymboldict.items() if spec[2] == sectype}
    Yahoodata.updatedf_symboldict(symboldict, symbollist_test, vecdelta)
    export_date = datetime.now() - timedelta(hours=tzoffset)
    if export_date.weekday() >= export_wkday - 1:
        Yahoodata.export_csv_symbollist(sectype)
        
logging.shutdown()