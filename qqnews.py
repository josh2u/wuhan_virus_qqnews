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
DICT_FILE = 'loc_translate.pickle'

# ## TODO change this to MYSQL 
# endpoint = 'xxx'
# db = MySQLDatabase('yy', user='xx', password='xx',
#                          host=endpoint, port=3306)

db = SqliteDatabase('qq.db')
################ TRANSLATION ################

loc_tran = None
try:
  with open(DICT_FILE, 'rb') as f:
    loc_tran = pickle.load(f)
except FileNotFoundError:
  print ('translation file not found.')
  loc_tran = {}
    

def zh_en_loc(name, cac):

  if name == None or name == '':
    return name
  
  tran_name = loc_tran.get(name, None)
  
  if tran_name:
    #if translation found, return
    return tran_name

  else:
    #if translation not found, get yandex translation and store to dictionary
    country, area, city = cac
    q = f'{country} # {area} # {city}'
    print (q)
    q = requests.utils.quote(q)
    url = f'https://translate.yandex.net/api/v1.5/tr.json/translate?text={q}&lang=zh-en&key=trnsl.1.1.20200130T145851Z.6e6374d5b42d9151.142c3045f7a677d6571c8602c098a600ff31df0e'
    resp = requests.post(url)
    jtxt = resp.json()

    chars_to_remove = [u'"', u"'", u'(', u')', u',']
    dd = {ord(c):None for c in chars_to_remove}

    tran_txt_clean = jtxt['text'][0].translate(dd)

    for zh, en in zip(cac, tran_txt_clean.split('#')):
      loc_tran[zh] = en.strip()

    with open(DICT_FILE, 'wb') as f:
      pickle.dump(loc_tran, f)

    return loc_tran[name]


    

################# DATABASE MODEL DEFINATION ##############

class QQNEWS_LIVE(Model):
  _id = AutoField(index=True, unique=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  cure_count = IntegerField(null=True)
  use_total = IntegerField(null=True)
  hint_words = CharField(null=True, index=True)
  recent_time = DateTimeField(index=True)
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

class QQNEWS_DAILY_SUMMARY(Model):
  _id = AutoField(index=True, unique=True)
  date =  DateField(index=True)
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
  country_en = CharField(null=True, index=True)
  area_en = CharField(null=True, index=True)
  city_en = CharField(null=True, index=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  heal_count = IntegerField(null=True)
  query_time = DateTimeField(index=True, default=datetime.now())

  class Meta:
    database = db

class QQNEWS_NEWS(Model):
  _id = AutoField(index=True, unique=True)
  time = DateTimeField(null=True)
  title = CharField(index=True)
  desc = CharField(null=True, index=True)
  source = CharField(null=True, index=True)
  create_time = DateTimeField(index=True, null=True ) 
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
print ('Scraping on :', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

for key, url in URLS.items():
  print ('requesting: ', url)
  resp = requests.get(url)
  j = resp.json()
  DATA[key] = resp.json().get('data', None)

# with open('sample_data.pkl', 'rb') as f:
#   DATA = pickle.load(f)


########### DATA PROCESSING AND SAVE TO DB ########################
## process Live
print ('processing live data ...')
glo = json.loads(DATA['global'])
for g in glo:
  recent_dt = datetime.strptime(g.get('recentTime', ''), '%Y-%m-%d %H:%M')
  ql = QQNEWS_LIVE(confirm_count = g.get('confirmCount', None),
              suspect_count = g.get('suspectCount', None),
              dead_count = g.get('deadCount', None),
              cure_count =  g.get('cure', None),
              use_total = g.get('useTotal', None),
              hint_words = g.get('hintWords', None),
              recent_time = recent_dt,
            )
  ql.save()
  print ('saved. ', ql._id)
  
  
## process daily summary
print ('processing daily summary ...')
daily = json.loads(DATA['day_count'])
for d in daily:
  dt = datetime.strptime(d.get('date', ''), '%m.%d').replace(year=datetime.now().year)
  dobj, iscreated = QQNEWS_DAILY_SUMMARY.get_or_create(
        date = dt,
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
  country = a.get('country', None)
  area = a.get('area', None)
  city = a.get('city', None)
  aobj = QQNEWS_AREA(
        country = country,
        area = area,
        city = city,
##TODO add translated version here
        country_en = zh_en_loc(country, (country, area, city)), # more accurate translation with context
        area_en = zh_en_loc(area, (country, area, city)),
        city_en = zh_en_loc(city, (country, area, city)),

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

  try:
    new_time = datetime.strptime(n.get('time', '12-31 23:59'), '%m-%d %H:%M').replace(year=datetime.now().year)
  except ValueError:
    new_time = datetime(1000, 1, 1, 0,0,0)

  try:
    create_dt = datetime.strptime(n.get('create_time', '1000-01-01T00:00:00.000Z'), '%Y-%m-%dT%H:%M:%S.000Z')
  except ValueError:
    create_dt = datetime(1000,1,1,0,0,0)

  nobj, iscreated = QQNEWS_NEWS.get_or_create(
          time = new_time,
          title = n.get('title', None),
          desc = n.get('desc', None),
          source = n.get('source', None),
          create_time = create_dt,
        )
  if iscreated:
    print ('save NEWS id:', nobj._id)
  else:
    print ('Create NEWS summary failed. Possibly exists:',  n.get('title', 'Null'))


print ('end of script')