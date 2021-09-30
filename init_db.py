# -*- coding: utf-8 -*-
"""
Created on Sun Aug 30 15:59:01 2020

@author: willbillion
"""

import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append('..')
from glob import glob
import shutil
from litedata import Litedata, basepath

def init_dbsetup():
    if not os.path.exists(basepath):
        os.makedirs(basepath)
    list_symbolcsv = glob('lite_*.csv')
    Yahoodata = Litedata('yahoo')
    for csvfile in list_symbolcsv:
        shutil.copy(csvfile, os.path.join(Yahoodata.data_path, csvfile))
        sectype = csvfile[5:].replace('.csv', '')
        readdict_sectype = Yahoodata.get_symboldict_csv(sectype)
        Yahoodata.insert_from_symboldict(readdict_sectype)
        adjust_path = os.path.join(Yahoodata.data_path, '%s_adj'%(sectype))
        raw_path = os.path.join(Yahoodata.data_path, '%s_raw'%(sectype))
        for path in [adjust_path, raw_path]:
            if not os.path.exists(path):
                os.makedirs(path)
        
if __name__ == '__main__':
    init_dbsetup()