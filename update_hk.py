import os
import sys
sys.path.append('..')

from litedata import basepath
from litedata.setupdb import init_dbsetup
dbfile = os.path.join(basepath, 'global_data.db')
if not os.path.exists(dbfile):
    init_dbsetup()
    
from datetime import datetime
currenttime = datetime.now().strftime('%Y%m%d')
import logging
logfile = f'liteupdate_{currenttime}.txt'
logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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