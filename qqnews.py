#!/usr/bin/env python
import requests
import json
from peewee import *
from datetime import datetime
import pickle

URLS = {
  'global' : 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_global_vars',
  'day_count': 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_cn_day_counts',
  'area_count': 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_area_counts',
  'news' : 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_time_line'
}

DATA = {}

## TODO change this to MYSQL 

# db = MySQLDatabase('QQNews', user='app', password='db_password',
#                          host='10.1.0.8', port=3316)

db = SqliteDatabase('qq.db')

################# DATABASE MODEL DEFINATION ##############

class QQNEWS_LIVE(Model):
  _id = AutoField(index=True, unique=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  cure_count = IntegerField(null=True)
  use_total = IntegerField(null=True)
  hint_words = TextField(null=True, index=True)
  recent_time = DateTimeField(index=True)
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

class QQNEWS_DAILY_SUMMARY(Model):
  _id = AutoField(index=True, unique=True)
  date = DateField(formats=['%m:%y'], unique=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  heal_count = IntegerField(null=True)
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

class QQNEWS_AREA(Model):
  _id = AutoField(index=True, unique=True)
  country = CharField(null=True, index=True)
  area = CharField(null=True, index=True)
  city = CharField(null=True, index=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  heal_count = IntegerField(null=True)
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

class QQNEWS_NEWS(Model):
  _id = AutoField(index=True, unique=True)
  time = DateTimeField(formats=['%m:%y %H:%M'], null=True)
  title = TextField(unique=True, index=True)
  desc = TextField(null=True, index=True)
  source = TextField(null=True, index=True)
  create_time = DateTimeField(formats=['%Y-%m-%dT%H:%M:%S.%f%z'], null=True, index=True, ) 
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

TABLE_NAMES = {'qqnews_live': QQNEWS_LIVE,
            'qqnews_daily_summary' : QQNEWS_DAILY_SUMMARY,
            'qqnews_area' : QQNEWS_AREA,
            'qqnews_news': QQNEWS_NEWS}

# Connect to DB, if table not exist, create it.
db.connect()
create_table = list(set(TABLE_NAMES.keys()) - set (db.get_tables()))
if create_table:
  print ('Table not exist.. create it')
  tables = map(lambda x: TABLE_NAMES[x], create_table)
  db.create_tables(tables)
  print ('Created table :', create_table)


############ CONNECT TO QQ NEWS TO GET DATA #####################

# http request from qq news
for key, url in URLS.items():
  print ('requesting: ', url)
  resp = requests.get(url)
  j = resp.json()
  DATA[key] = resp.json().get('data', None)

# with open('sample_data.pkl', 'rb') as f:
#   DATA = pickle.load(f)


############ DATA PROCESSING AND SAVE TO DB ########################
## process Live
print ('processing live data ...')
glo = json.loads(DATA['global'])
for g in glo:
  ql = QQNEWS_LIVE(confirm_count = g.get('confirmCount', None),
              suspect_count = g.get('suspectCount', None),
              dead_count = g.get('deadCount', None),
              cure_count =  g.get('cure', None),
              use_total = g.get('useTotal', None),
              hint_words = g.get('hintWords', None),
              recent_time = g.get('recentTime', None),
            )
  ql.save()
  
  
## process daily summary
print ('processing daily summary ...')
daily = json.loads(DATA['day_count'])
for d in daily:
  dobj, iscreated = QQNEWS_DAILY_SUMMARY.get_or_create(
        date = d.get('date', None),
        confirm_count = d.get('confirm', None),
        suspect_count = d.get('suspect', None),
        dead_count = d.get('dead', None),
        heal_count = d.get('heal', None),
        )
  if iscreated:
    print ('save daily id:', dobj._id)
  else:
    print ('Create DAILY SUMMARY failed. Possibly exists:',  d.get('date', 'Null'))


## Area infection update
# Area will append a new data each run
print ('processing area update ...')
arealist = json.loads(DATA['area_count'])
for a in arealist:
  aobj = QQNEWS_AREA(
        country = a.get('country', None),
        area = a.get('area', None),
        city = a.get('city', None),
        confirm_count = a.get('confirm', None),
        suspect_count = a.get('suspect', None),
        dead_count = a.get('dead', None),
        heal_count = a.get('heal', None)
      )
  aobj.save()
  print ('saved ', a.get('country', 'Null'), a.get('area', 'Null'), a.get('city', 'Null'))


## Process News, Title set to unique, same title will only has 1 entry
print ('processing news ...')
news = json.loads(DATA['news'])
for n in news:
  nobj, iscreated = QQNEWS_NEWS.get_or_create(
          time = n.get('time', None),
          title = n.get('title', None),
          desc = n.get('desc', None),
          source = n.get('source', None),
          create_time = n.get('create_time', None),
        )
  if iscreated:
    print ('save NEWS id:', nobj._id)
  else:
    print ('Create NEWS summary failed. Possibly exists:',  n.get('title', 'Null'))


print ('end of script')