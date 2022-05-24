import ast,requests

groupid='ENTER GROUP ID HERE'
token = 'ENTER YOUR TOKEN HERE'
txt = open('id.txt', 'w')
params = {'group_id':groupid,'v':'5.131','access_token': token}
url = 'https://api.vk.com/method/groups.getMembers'
merged_list =[]
HEADERS={
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'
}
def get_html(url,params=''):
    res = requests.get(url,headers=HEADERS,params=params)
    finalb = res.content
    return finalb

def bytes_to_dict(bytes):
    mydata = ast.literal_eval(bytes.decode("UTF-8"))
    return mydata

counter = bytes_to_dict(get_html(url,params))['response']['count']//1000 + 1

for o in range(0, counter+1):
    txt = open('id.txt', 'w')
    allusers =  get_html(url,params={'group_id':groupid,'offset': o*1000,'v':'5.131','access_token': token})
    rdict = bytes_to_dict(allusers)
    ids = rdict['response']['items']
    merged_list.extend(ids)
    
    for item in merged_list:
        txt.write('vk.com/id'+ str(item)+ '\n')

  

