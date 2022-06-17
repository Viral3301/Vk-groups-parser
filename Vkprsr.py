import requests,os,sqlite3,time
import pandas as pd
from sqlalchemy import create_engine
from config import *
from dotenv import load_dotenv
load_dotenv()


groupid='ENTER GROUP ID'
token = os.getenv('token')
params = {'group_id':groupid,'fields':'contacts','v':'5.131','access_token': token}
user_params = {'user_ids':groupid,'fields':'contacts','v':'5.131','access_token': token}
userinf_list = []

userid_list = []
connect = sqlite3.connect('vkdata.db')
cursor = connect.cursor()
connect.execute("PRAGMA foreign_keys = ON;")
engine = create_engine('sqlite:///vkdata.db')
engine.connect()

cursor.execute("""CREATE TABLE IF NOT EXISTS groupinfo (rowid integer PRIMARY KEY, id integer ,name text, description text, country text, site text,screen_name text,is_closed text,type text,is_admin text,is_member text,is_advertiser text,photo_50 text,photo_100 text,photo_200 text);""")
cursor.execute("""CREATE TABLE IF NOT EXISTS group_members (user_id integer, group_id integer, FOREIGN KEY (group_id) REFERENCES groupinfo (rowid));""")
cursor.execute("""CREATE TABLE IF NOT EXISTS users (rowid integer PRIMARY KEY,id integer, first_name text, last_name text,bdate text,can_be_invited_group text,sex integer,verified integer,books text,mobile_phone text,followers_count integer,activities text,can_access_closed text,is_closed text,about text,home_phone text);""")
cursor.execute("""CREATE TABLE IF NOT EXISTS"schools" (counter integer PRIMARY KEY,UID integer,"city"	integer,"class"	TEXT,	"country"	integer,	"id"	integer,	"name"	TEXT,	"year_graduated"	integer,	"year_to"	integer,	"type"	REAL,	"type_str"	TEXT,	"year_from"	integer,	"speciality"	TEXT);""")
cursor.execute("""CREATE TABLE IF NOT EXISTS"career" ("UID" INTEGER PRIMARY KEY,"city_id" integer,"company" TEXT,"country_id" integer,"from" integer,"position" TEXT,"until" integer,"group_id" integer);""")

def get_groupinfo():
    description = requests.get('https://api.vk.com/method/groups.getById',headers=HEADERS,params={'group_id': groupid,'fields':'description,country,site','access_token': token,'v':'5.131'}).json()
    return description

AllGroupinfo = get_groupinfo()['response'][0]

group_df = pd.DataFrame(AllGroupinfo,index=[1])
print(group_df)
group_df.to_sql('groupinfo', con=connect,if_exists='append',index=False)





def get_html(url,params):
    content = requests.get(url,headers=HEADERS,params=params)
    return content.json()

User_counter= get_html(url,params)['response']['count']//1000 + 1

for o in range(0, User_counter+1):
    AllUser_data =  get_html(url,params={'group_id':groupid,'offset': o*1000,'fields':'contacts','v':'5.131','access_token': token})['response']['items']
    for item in AllUser_data:
        userid_list.append(item['id'])

cursor.execute("SELECT * FROM groupinfo ORDER BY rowid DESC LIMIT 1;")
last =cursor.fetchone()
print(last[0])

userid_df = pd.DataFrame(userid_list).rename(columns={0: 'user_id'})
uuserid_df= userid_df.assign(group_id = last[0])

def get_byid(*id):
    user_params = {'user_ids':id,'fields': fieldds,'v':'5.131','access_token': token}
    alluserinfo = get_html(getbyid_url, params=user_params)
    return alluserinfo

countlimit = 1000
countfrom = 0
for item in range(0,len(userid_list)//1000 + 1):
    
    info = get_byid(str(userid_list[countfrom:countlimit]))['response']
    countfrom += 1000
    countlimit += 1000
    for item in info:
        userinf_list.append(item)
    time.sleep(1)

userinf_df = pd.DataFrame(userinf_list)
userinf_df.drop(columns=['career','schools']).to_sql('users', con=connect,if_exists='append',index=False)
school_list = userinf_df['schools'].apply(pd.Series)[0]
uid_column = userinf_df["id"].rename('UID')
normal_list = pd.json_normalize(school_list)
nl = pd.concat([uid_column,normal_list],axis=1).dropna(subset=['id'])

career_list = userinf_df['career'].apply(pd.Series)[0]
normal_career_list = pd.json_normalize(career_list)
ncl = pd.concat([uid_column,normal_career_list],axis=1).dropna(subset=['city_id'])




ncl.to_sql('career', con=connect,if_exists='append',index=False)
nl.to_sql('schools', con=connect,if_exists='append',index=False)
uuserid_df.to_sql('group_members', con=connect,if_exists="append",index=False)







    




