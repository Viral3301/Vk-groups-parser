import requests,os
from config import *
from dotenv import load_dotenv
load_dotenv()


groupid='mwt_mlp'
token = os.getenv('token')
params = {'group_id':groupid,'fields':'contacts,bdate,country','v':'5.131','access_token': token}

def get_groupinfo():
    description = requests.get('https://api.vk.com/method/groups.getById',headers=HEADERS,params={'group_id': groupid,'fields':'description','access_token': token,'v':'5.131'}).json()
    return description

AllGroupinfo = get_groupinfo()['response'][0]
GroupDescription = [AllGroupinfo['id'],AllGroupinfo['description'],AllGroupinfo['name']] #Cписок id,описание,название группы

def get_html(url,params):
    content = requests.get(url,headers=HEADERS,params=params)
    return content.json()

User_counter= get_html(url,params)['response']['count']//1000 + 1

for o in range(0, User_counter+1):
    AllUser_data =  get_html(url,params={'group_id':groupid,'offset': o*1000,'fields':'contacts,bdate,country','v':'5.131','access_token': token})['response']['items']
    for user in AllUser_data:
        response_list = [user['id'],user['first_name'],user['last_name']]
        print(response_list)