import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday
from modules.general import ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
import pandas as pd
from modules.connection import OracleCon
import time
from datetime import datetime


'''
today=GetToday()
dt_string=ConvertDatetoStr(today,'%Y-%m-%d')
hourlyscp=pd.read_csv('./rawdata/scp_today_hourly.csv')
minutscp=pd.read_csv('./rawdata/scp_today_minute.csv')
hourlysdp=pd.read_csv('./rawdata/sdp_today_hourly.csv')
minutsdp=pd.read_csv('./rawdata/sdp_today_minute.csv')
df = pd.DataFrame(columns=['A'])
print(len(hourlyscp.index))

print('hallllloooooo !!!!')'''

cred={"host": "10.64.27.31","port":"1523","username":"OPS_RECON","password":"Ops_r3c0N","sid":"SCM"}

while True :
    conn=OracleCon(patfile="sdpconfig.json")
    print(conn)
    today=GetToday()
    dt=datetime.strftime(today,'%Y-%m-%d %H:%M:%S')
    print('{} : hallllloooooo !!!!'.format(dt))
    time.sleep(30)


