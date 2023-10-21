import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import time
from datetime import timedelta
from modules.connection import OracleCon
from modules.GetData import GetDataNow
from modules.DataProcess import ScpData,SdpData
import pandas as pd


#GetDataNow()
pathdir=os.getcwd()
rawscp=f'{pathdir}/rawdata/sdp_data_raw.csv'
datascp=SdpData(pathfile=rawscp)
att,suc,scr=datascp.SumDataToday(accflag=73)
listatt,listsuc,listhour=datascp.HourlyDataToday(accflag=73)
os.system('clear')
print(att,suc,scr)
print(listatt)
print(listsuc)
print(listhour)