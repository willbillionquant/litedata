import os
codepath = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append('..')

from datetime import datetime, timedelta

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

# US trading days setup
holidaydictny = {
    2014: ['-01-01', '-01-20', '-02-17', '-04-18', '-05-26', '-07-04', '-09-01', '-11-27', '-12-25'],
    2015: ['-01-01', '-01-19', '-02-16', '-04-03', '-05-25', '-07-03', '-09-07', '-11-26', '-12-25'],
    2016: ['-01-01', '-01-18', '-02-15', '-03-25', '-05-30', '-07-04', '-09-05', '-11-24', '-12-26'],
    2017: ['-01-02', '-01-16', '-02-20', '-04-14', '-05-29', '-07-04', '-09-04', '-11-23', '-12-25'],
    2018: ['-01-01', '-01-15', '-02-19', '-03-30', '-05-28', '-07-04', '-09-03', '-11-22', '-12-25'],
    2019: ['-01-01', '-01-21', '-02-18', '-04-19', '-05-27', '-07-04', '-09-02', '-11-28', '-12-25'],
    2020: ['-01-01', '-01-20', '-02-17', '-04-10', '-05-25', '-07-03', '-09-07', '-11-26', '-12-25'],
    2021: ['-01-01', '-01-18', '-02-15', '-04-02', '-05-31', '-07-05', '-09-06', '-11-25', '-12-24'],
    2022: ['-01-17', '-02-21', '-04-15', '-05-30', '-06-20', '-07-04', '-09-05', '-11-24', '-12-26'],
    2023: ['-01-02', '-01-16', '-02-20', '-04-07', '-05-29', '-06-19', '-07-04', '-09-04', '-11-23', '-12-25'],
}

holidaydictny = {year: [f'{str(year)}{dtstr}' for dtstr in vallist] for year, vallist in holidaydictny.items()}

holidaydicthk = {

    2013: ['-01-01', '-02-11', '-02-12', '-02-13', '-03-29', '-04-01', '-04-04', '-05-01', '-05-17', '-06-12',
           '-07-01', '-09-20', '-10-01', '-10-14', '-12-25', '-12-26'],

    2014: ['-01-01', '-01-31', '-02-03', '-04-18', '-04-21', '-05-01', '-05-06', '-06-02', '-07-01', '-09-09',
           '-10-01', '-10-02', '-12-25', '-12-26'],

    2015: ['-01-01', '-02-19', '-02-20', '-04-03', '-04-06', '-04-07', '-05-01', '-05-25', '-07-01', '-09-03',
           '-09-28', '-10-01', '-10-21', '-12-25'],

    2016: ['-01-01', '-02-08', '-02-09', '-02-10', '-03-25', '-03-28', '-04-04', '-05-02', '-06-09', '-07-01',
           '-09-16', '-10-10', '-12-26', '-12-27'],

    2017: ['-01-02', '-01-30', '-01-31', '-04-04', '-04-14', '-04-17', '-05-01', '-05-03', '-05-30', '-10-02',
           '-10-05', '-12-25', '-12-26'],

    2018: ['-01-01', '-02-16', '-02-19', '-03-30', '-04-02', '-04-05', '-05-01', '-05-22', '-06-18', '-07-02',
           '-09-25', '-10-01', '-10-17', '-12-25', '-12-26'],

    2019: ['-01-01', '-02-05', '-02-06', '-02-07', '-04-05', '-04-19', '-04-22', '-05-01', '-05-13', '-06-07',
           '-07-01', '-10-01', '-10-07', '-12-25', '-12-26'],

    2020: ['-01-01', '-01-27', '-01-28', '-04-10', '-04-13', '-04-30', '-05-01', '-06-25', '-07-01', '-10-01',
           '-10-02', '-10-13', '-10-26', '-12-25'],

    2021: ['-01-01', '-02-12', '-02-15', '-04-02', '-04-05', '-04-06', '-05-19', '-06-14', '-07-01', '-09-22',
           '-10-01', '-10-13', '-10-14', '-12-27'],

    2022: ['-02-01', '-02-02', '-02-03', '-04-05', '-04-15', '-04-18', '-05-02', '-05-09', '-06-03', '-07-01',
           '-09-12', '-10-04', '-12-26', '-12-27'],

    2023: ['-01-02', '-01-23', '-01-24', '-01-25', '-04-05', '-04-07', '-04-10', '-05-01', '-05-26', '-06-22',
           '-10-02', '-10-23', '-12-25', '-12-26']

}

holidaydicthk = {year: [f'{str(year)}{dtstr}' for dtstr in vallist] for year, vallist in holidaydicthk.items()}

def getworkdays(startyr=2015, endyr=2022, form=0):
    """Get all dates  to string in format 0 ('yyyy-mm-dd') or 1 ('yyyymmdd'') or 2 ('yymmdd)."""
    assert (form in [0, 1, 2]), 'Inappropriate form!'
    dtlist = []
    ltdate = datetime.strptime(str(startyr) + '-01-01', '%Y-%m-%d')
    while (ltdate.year >= startyr) and (ltdate.year <= endyr):
        if ltdate.weekday() <= 4:
            ltdtstr = ltdate.strftime('%Y-%m-%d')
            if form == 0:
                dtlist.append(ltdtstr)
            elif form == 1:
                dtlist.append(ltdtstr.replace("-", ""))
            elif form == 2:
                dtlist.append(ltdtstr.replace("-", "")[2:])
        ltdate += timedelta(days=1)

    return dtlist

def gettradedays(holidaydict, form=0):
    """Get all trading day spanning a couple of years."""
    assert (form in [0, 1, 2]), 'Inappropriate form!'
    yearlist = list(holidaydict.keys())
    yearlist.sort()
    workdtlist = getworkdays(yearlist[0], yearlist[-1], form)
    tdlist = workdtlist.copy()
    holidaylist = []
    for year in range(yearlist[0], yearlist[-1] + 1):
        hdaylist = holidaydict[year]
        if form == 1:
            hdaylist = [name.replace("-", "") for name in hdaylist]
        elif form == 2:
            hdaylist = [name.replace("-", "")[2:] for name in hdaylist]
        holidaylist += hdaylist

    for date in holidaylist:
        if date in tdlist:
            tdlist.remove(date)
        else:
            print(f'{date} NOT in working day list.')

    return tdlist

## Resample to other frequency
def ohlcresample(dfohlc, assetlist, freq='W'):
    """Resample the OHLC dataframe into desired timeframe."""
    aggrule = {}
    for asset in assetlist:
        aggrule[f'{asset}_op'] = 'first'
        aggrule[f'{asset}_hi'] = 'max'
        aggrule[f'{asset}_lo'] = 'min'
        aggrule[f'{asset}_cl'] = 'last'
        aggrule[f'{asset}_vol'] = 'sum'
    dfnew = dfohlc.resample(rule=freq, label='left').agg(aggrule)

    return dfnew