import requests,os,sqlite3,time
import pandas as pd
from sqlalchemy import create_engine
from config import *
from dotenv import load_dotenv
load_dotenv()


groupid='mwt_mlp'
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

cursor.execute("""CREATE TABLE IF NOT EXISTS groupinfo ( id integer PRIMARY KEY, group_name text, description text, country text, site text);""")
cursor.execute("""CREATE TABLE IF NOT EXISTS group_members (user_id integer, group_id integer, FOREIGN KEY (group_id) REFERENCES groupinfo (id));""")
cursor.execute("""CREATE TABLE IF NOT EXISTS users (rowid integer PRIMARY KEY,id integer, first_name text, last_name text,bdate text,can_be_invited_group text,sex integer,verified integer,books text,career integer,schools text,mobile_phone text,followers_count integer,activities text,can_access_closed text,is_closed text,about text,home_phone text);""")


connect.commit()

def get_groupinfo():
    description = requests.get('https://api.vk.com/method/groups.getById',headers=HEADERS,params={'group_id': groupid,'fields':'description,country,site','access_token': token,'v':'5.131'}).json()
    return description

AllGroupinfo = get_groupinfo()['response'][0]
GroupDescription = { 'description': AllGroupinfo['description'],'group_name': AllGroupinfo['name'],'country': AllGroupinfo['country']['title'],'site': AllGroupinfo['site']} #Cписок id,описание,название группы

group_df = pd.DataFrame(GroupDescription,index=[0])
group_df.to_sql('groupinfo', con=connect,if_exists='append',index=False)





def get_html(url,params):
    content = requests.get(url,headers=HEADERS,params=params)
    return content.json()

User_counter= get_html(url,params)['response']['count']//1000 + 1

for o in range(0, User_counter+1):
    AllUser_data =  get_html(url,params={'group_id':groupid,'offset': o*1000,'fields':'contacts','v':'5.131','access_token': token})['response']['items']
    for item in AllUser_data:
        userid_list.append(item['id'])

cursor.execute("SELECT * FROM groupinfo ORDER BY id DESC LIMIT 1;")
last =cursor.fetchone()
print(last[0])

userid_df = pd.DataFrame(userid_list).rename(columns={0: 'user_id'})
uuserid_df= userid_df.assign(group_id = last[0])
uuserid_df.to_sql('group_members', con=connect,if_exists="append",index=False)
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
    userinf_list.append(info)
    time.sleep(1)


for item in range(0,len(userinf_list)):
    userinfo_df = pd.DataFrame(userinf_list[item])

    finaldf = userinfo_df.drop(columns=['career','schools'],axis=1).rename(columns={id:'user_id'})
    schools = userinfo_df.get('schools').apply(pd.Series).drop(columns=[1,2],axis=1,errors='ignore').rename(columns={0: 'schools'})
    s = schools['name'] = schools['schools'].str['name']
    career = userinfo_df['career'].apply(pd.Series).drop(columns=[1,2],axis=1,errors='ignore').rename(columns={0: 'career'})
    f = career['group_id'] = career['career'].str['group_id']
    finaldf = pd.concat([finaldf,f,s],axis=1)
    finaldf.to_sql('users', con=connect,if_exists='append',index=False)
    




    




