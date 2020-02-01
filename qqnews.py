import requests
import json
from peewee import MySQLDatabase, SqliteDatabase, Model, AutoField, CharField, IntegerField, DateTimeField,DateField
from datetime import datetime, timedelta
import pickle

URLS = {
  'all': 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5',
  'news' : 'https://view.inews.qq.com/g2/getOnsInfo?name=wuwei_ww_time_line'
}

DATA = {}

# endpoint = 'xx'
# db = MySQLDatabase('xx', user='xx', password='xx', connect_timeout=20,
#                          host=endpoint, port=3306)


db = SqliteDatabase('qq.db')

dtnow = datetime.now()
################# DATABASE MODEL DEFINATION ##############

class QQNEWS_LIVE(Model):
  _id = AutoField(index=True, unique=True)
  date = CharField(null=True, index=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  cure_count = IntegerField(null=True)
  use_total = IntegerField(null=True)
  hint_words = CharField(null=True, index=True)
  recent_time = DateTimeField(index=True)
  query_time = DateTimeField(index=True, default=dtnow)

  class Meta:
    database = db

class QQNEWS_DAILY_SUMMARY(Model):
  _id = AutoField(index=True, unique=True)
  date =  DateField(index=True)
  confirm_count = IntegerField(null=True)
  suspect_count = IntegerField(null=True)
  dead_count = IntegerField(null=True)
  heal_count = IntegerField(null=True)
  query_time = DateTimeField(index=True, default=dtnow)

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
  confirm_today_count = IntegerField(null=True)
  suspect_today_count = IntegerField(null=True)
  dead_today_count = IntegerField(null=True)
  heal_today_count = IntegerField(null=True)
  sourceLastUpdateTime = DateTimeField(index=True)
  query_time = DateTimeField(index=True, default=dtnow)

  class Meta:
    database = db

class QQNEWS_NEWS(Model):
  _id = AutoField(index=True, unique=True)
  time = DateTimeField(null=True)
  title = CharField(index=True)
  desc = CharField(null=True, index=True)
  source = CharField(null=True, index=True)
  create_time = DateTimeField(index=True, null=True ) 
  query_time = DateTimeField(index=True, default=dtnow)

  class Meta:
    database = db

TABLE_NAMES = {'qqnews_live': QQNEWS_LIVE,
            'qqnews_daily_summary' : QQNEWS_DAILY_SUMMARY,
            'qqnews_area' : QQNEWS_AREA,
            'qqnews_news': QQNEWS_NEWS}


############# Connect to db and create table if doesn't exist ###############

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

data = json.loads(DATA['all'])
last_update_time = datetime.strptime(data['lastUpdateTime'], '%Y-%m-%d %H:%M:%S') - timedelta(hours=8)


####### load dictionary #########
DICT_FILE = 'loc_translate.pickle'
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


###### LIVE ##########
print ('processing live ...')
g = data['chinaTotal']
dt = datetime.strptime(g.get('date', ''), '%m.%d').replace(year=datetime.now().year)
ql = QQNEWS_LIVE(
        date = dt, #TODO add column
        confirm_count = g.get('confirm', None),
        suspect_count = g.get('suspect', None),
        dead_count = g.get('dead', None),
        cure_count =  g.get('heal', None),
        use_total = None,
        hint_words = None,
        recent_time = last_update_time,
            
      )
ql.save()


########## DAILY ##########
daily = data['chinaDayList']


## process daily summary
print ('processing daily summary ...')
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



############ AREA ##############

def processAreaTree(tree):

  print ("parsing tree ...")
  
  arealist = []
  # Country -> Area -> City
  for country in tree:
    res = {}

    if 'children' in country:

      for area in country['children']:

        if 'children' in area:

          for city in area['children']:
            co, ar, ci = country['name'], area['name'], city['name']
            cac = (co,ar,ci)

            res['country'], res['country_en'] = co, zh_en_loc(co, cac)
            res['area'], res['area_en'] = ar, zh_en_loc(ar, cac)
            res['city'], res['city_en'] = ci, zh_en_loc(ci, cac)

            res['total'] = city['total']
            res['today'] = city['today']

            arealist.append(res)
            res = {}

        else: # no city, only area
          co, ar, ci = country['name'], area['name'], None
          cac = (co,ar,ci)
          
          res['country'], res['country_en'] = co, zh_en_loc(co, cac)
          res['area'], res['area_en'] = ar, zh_en_loc(ar, cac)
          res['city'], res['city_en'] = None, None


          res['total'] = area['total']
          res['today'] = area['today']

          arealist.append(res)
          res = {}

    else: # no area , only country

      co, ar, ci = country['name'], None, None
      cac = (co,ar,ci)

      res['country'], res['country_en'] = co, zh_en_loc(co, cac)
      res['area'], res['area_en'] = None, None
      res['city'], res['city_en'] = None, None
      
      res['total'] = country['total']
      res['today'] = country['today']

      arealist.append(res)
      res = {}

  return arealist

tree = data['areaTree']
arealist = processAreaTree(tree)

print ('processing area update ...')
for re in arealist:
  aobj = QQNEWS_AREA(
        country = re['country'],
        area = re['area'],
        city = re['city'],
        country_en = re['country_en'],
        area_en = re['area_en'],
        city_en = re['city_en'],
        confirm_count = re['total']['confirm'],
        suspect_count = re['total']['suspect'],
        dead_count = re['total']['dead'],
        heal_count = re['total']['heal'],
        confirm_today_count = re['today']['confirm'],
        suspect_today_count = re['today']['suspect'],
        dead_today_count = re['today']['dead'],
        heal_today_count = re['today']['heal'],
        sourceLastUpdateTime = last_update_time
      )
  aobj.save()
  print ('saved ', re['country'], re['area'], re['city'] )




## Process News, Title set to unique, same title will only has 1 entry
print ('processing news ...')
news = json.loads(DATA['news'])
for n in news:

  try:
    new_time = datetime.strptime(n.get('time', '12-31 23:59'), '%m-%d %H:%M').replace(year=datetime.now().year) - timedelta(hours=8)
  except ValueError:
    new_time = datetime(1000, 1, 1, 0,0,0)

  try:
    create_dt = datetime.strptime(n.get('create_time', '1000-01-01T00:00:00.000Z'), '%Y-%m-%dT%H:%M:%S.000Z') - timedelta(hours=8)
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


print ('Ended at: ', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
