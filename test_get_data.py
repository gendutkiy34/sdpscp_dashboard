import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,ConvertStrtoDate
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import time
from datetime import timedelta
from modules.connection import OracleCon
from modules.GetData import GetDataNow,GetDataNew,GetData3DCur
from modules.DataProcess import ScpData,SdpData
import pandas as pd



#d0=ConvertStrtoDate('2023-10-22',format='%Y-%m-%d')
today=GetToday()
GetData3DCur(day0=today,env='scp')
#GetDataNew(env='sdp')
#pathdir=os.getcwd()
#rawscp=f'{pathdir}/rawdata/scp_data_3day.csv'
#datascp=ScpData(pathfile=rawscp)
#list1,list2,list3,listh=datascp.AttSk3Days(servicekey=150)
#os.system('clear')
#print(list1,list2,list3,listh)


'''
############## TEST DATA
pathdir=os.getcwd()
today=GetToday()
dt1=today - timedelta(minutes=5)
dt2=today - timedelta(minutes=1)
mon=ConvertDatetoStr(tgl=today,format='%m')
day=ConvertDatetoStr(tgl=today,format='%d')
tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
conpath=(f'{pathdir}/connections/sdpprodtrx.json')
sqltxt=ReadTxtFile(f'{pathdir}/sql/sdptrafficupdate.sql')
output=f'{pathdir}/rawdata/sdp_data_raw.csv'
list_column=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']
sql=sqltxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
conn=OracleCon(conpath)
cur=conn.cursor()
tempdata=cur.execute(sql)
print(tempdata)'''