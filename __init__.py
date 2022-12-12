import os
codepath = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append('..')

from configparser import ConfigParser
configdata = ConfigParser()
configdata.read(os.path.join(codepath, 'setting_litedata.ini'))

# File path
basepath = configdata['paths'].get('basepath')
logpath = os.path.join(basepath, 'logs')
for path in [basepath, logpath]:
    if not os.path.exists(path):
        os.makedirs(path)

# OHLC fields
ohlcfield = ['op', 'hi', 'lo', 'cl']
ohlcvdfield = ['op', 'hi', 'lo', 'cl', 'vol', 'div']