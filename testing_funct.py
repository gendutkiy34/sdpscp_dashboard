import os
<<<<<<< HEAD
from modules.DataProcess import ScpData
from modules.general import GetToday
=======
import pandas as pd
import time
from datetime import datetime
from modules.connection import OracleCon
from modules.DataProcess import ScpData,SdpData


path=os.getcwd()
raw=f'{path}/rawdata/scp_data_3day.csv'
#d0=ConvertStrtoDate('2023-10-25',format='%Y-%m-%d')
#raw=f'{path}/rawdata/scp_data_raw.csv'
datascp=ScpData(pathfile=raw)
dftoday=datascp.VerifyData()
list1,list2,list3=datascp.SumDataToday()
print(dftoday.head())
print(list1)
print(list2)
print(list3)
#print(listh)
>>>>>>> d962ddace36749610caa0b4aa3425249a32ba9b9

path=os.getcwd()

today=GetToday()
<<<<<<< HEAD
rawscp=f'{path}/rawdata/scp_data_raw.csv'
datascp=ScpData(pathfile=rawscp)
data1=datascp.VerifyData()
data2=data1[data1['DATE'] == today.date()]
print(data2.tail())
=======
dt_string=ConvertDatetoStr(today,'%Y-%m-%d')
hourlyscp=pd.read_csv('./rawdata/scp_today_hourly.csv')
minutscp=pd.read_csv('./rawdata/scp_today_minute.csv')
hourlysdp=pd.read_csv('./rawdata/sdp_today_hourly.csv')
minutsdp=pd.read_csv('./rawdata/sdp_today_minute.csv')
df = pd.DataFrame(columns=['A'])
print(len(hourlyscp.index))

print('hallllloooooo !!!!')

cred={"host": "10.64.27.31","port":"1523","username":"OPS_RECON","password":"Ops_r3c0N","sid":"SCM"}

while True :
    conn=OracleCon(patfile="sdpconfig.json")
    print(conn)
    today=GetToday()
    dt=datetime.strftime(today,'%Y-%m-%d %H:%M:%S')
    print('{} : hallllloooooo !!!!'.format(dt))
    time.sleep(30)'''

>>>>>>> d962ddace36749610caa0b4aa3425249a32ba9b9

