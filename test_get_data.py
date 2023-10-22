import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,ConvertStrtoDate
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import time
from datetime import timedelta
from modules.connection import OracleCon
from modules.GetData import GetDataNow,GetData3Days
from modules.DataProcess import ScpData,SdpData
import pandas as pd



#d0=ConvertStrtoDate('2023-10-22',format='%Y-%m-%d')
#GetData3Days(day0=d0,env='sdp')
#GetDataNow()
pathdir=os.getcwd()
rawscp=f'{pathdir}/rawdata/scp_data_3day.csv'
datascp=ScpData(pathfile=rawscp)
list1,list2,list3,listh=datascp.AttSk3Days(servicekey=150)
os.system('clear')
print(list1,list2,list3,listh)