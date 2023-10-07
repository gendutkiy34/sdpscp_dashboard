import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from formapp import FormHttpReq,FormLog,FormCdr,FormCpId
from modules.htttprequest import ReqHttp
from modules.scplog import GetScpLog,ExtractScpLog
from modules.sdplog import GetSdpLog
from modules.ReadSdpLog import ExtractScmLog
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
from modules.extractcdr import ExtractCdrSdp
import time
from datetime import timedelta
from modules.connection import OracleCon
import pandas as pd


while True :
    today=GetToday()
    dt1=today - timedelta(minutes=18)
    dt2=today - timedelta(minutes=1)
    mon=ConvertDatetoStr(tgl=today,format='%m')
    day=ConvertDatetoStr(tgl=today,format='%d')
    tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
    tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
    scpcon=('./connections/scpprodtrx.json')
    sdpcon=('./connections/sdpprodtrx.json')
    sdpsql=ReadTxtFile('./sql/sdptraffictoday.sql')
    scpsql=ReadTxtFile('./sql/scptraffictoday.sql')
    sdpminute=ReadTxtFile('./sql/sdptrafficminute.sql')
    scpminute=ReadTxtFile('./sql/scptrafficminute.sql')
    datascp=GetPandasToday(conpath=scpcon,sqlraw=scpsql,tgl=today)
    datascp.to_csv('./rawdata/scp_today_hourly.csv',index=False)
    datasdp=GetPandasToday(conpath=sdpcon,sqlraw=sdpsql,tgl=today)
    datasdp.to_csv('./rawdata/sdp_today_hourly.csv',index=False)
    print('hourly done')
    sqlscpmin=scpminute.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    sqlsdpmin=sdpminute.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    try :
        conscp=OracleCon(scpcon)
        scpmin=pd.read_sql(sqlscpmin,con=conscp)
        consdp=OracleCon(sdpcon)
        sdpmin=pd.read_sql(sqlsdpmin,con=consdp)
    except Exception :
        pass
    scpmin.to_csv('./rawdata/scp_today_minute.csv',index=False)
    sdpmin.to_csv('./rawdata/sdp_today_minute.csv',index=False)
    print('get data finish')
    time.sleep(900)
    