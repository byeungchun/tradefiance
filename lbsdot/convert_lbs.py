import json
import logging
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from xmlparser import getKeyfamilies

log = logging.getLogger(__name__)
DT_FMT_YM = '%Y%m'
DT_FMT_YMD = '%Y%m%d'

def main(lbsfile, mg_collection):
  mgclient = MongoClient(unicode_decode_error_handler='ignore')
  mgdb = mgclient['lbsdot']
  mgcl_lbs = mgdb[mg_collection]

  log.info('Parsing({}) started'.format(lbsfile))
  lbs_parser = parse_lbs_file(lbsfile)
  for series_dict in lbs_parser:
    mgcl_lbs.insert_one(series_dict)
  log.info('Parsing({}) finished'.format(lbsfile))

def get_dim_from_dsd(bisdsd, kfname):
  dict_kf = getKeyfamilies(bisdsd)
  return dict_kf[kfname]['concept']

def generate_ts(line):
  obsprd = line[34:].split(':')[0]
  frdate = datetime.strptime('{}{}'.format(obsprd[:4], int(obsprd[4])*3),DT_FMT_YM)
  if len(line[34:].split(':')[0]) > 5: # one observation
    todate = datetime.strptime('{}{}'.format(obsprd[5:9], int(obsprd[9])*3),DT_FMT_YM)
    dtlist =  pd.date_range(frdate, todate + pd.offsets.QuarterBegin(1), freq='Q').strftime(DT_FMT_YMD).tolist()
    obsval = line[34+15:-1]
  else: # more than one observaton
    obsval = line[34+10:-1]
    dtlist = [frdate.strftime(DT_FMT_YMD)]
  obslist = obsval.split('+')
  for i, _obs in enumerate(obslist):
    if i == 0:
      res = list()
    _obs = _obs.split(':')[0] 
    if (len(_obs) != 0) and (_obs != '-'):
      res.append((int(dtlist[i]), float(_obs)))
  return res

def parse_lbs_file(lbsfile):
  dimlist = get_dim_from_dsd(r'E:\data\ibfs\meta\BIS-DSD.20.xml', 'BIS_LBS_DISS')
  is_contentline = False
  with open(lbsfile) as lbs_file:
    rawcnt = 0
    while True:
      line = lbs_file.readline().strip()
      if is_contentline:
        if line[:5] == 'ARR++':
          res = {'timeseries': generate_ts(line)}
          res.update(dict(zip(dimlist, line[5:33].split(':'))))
          yield res
        else:
          log.info('Parsing finished: {}'.format(lbsfile))
          break
      elif line[:5] == 'GIS+1':
        is_contentline = True


if __name__ == "__main__":
  main(r'E:\data\ibfs\source\LBSN.ges', 'raw_lbsn2')
