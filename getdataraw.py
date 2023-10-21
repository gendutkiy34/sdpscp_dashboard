import time
from datetime import timedelta
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.connection import OracleCon
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import pandas as pd
import time
import os


def GrepNewData():
    pathdir=os.getcwd()
    today=GetToday()
    dt1=today - timedelta(minutes=22)
    dt2=today - timedelta(minutes=2)
    mon=ConvertDatetoStr(tgl=today,format='%m')
    day=ConvertDatetoStr(tgl=today,format='%d')
    tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
    tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
    scpcon=('{0}/connections/scpprodtrx.json'.format(pathdir))
    sdpcon=('{0}/connections/sdpprodtrx.json'.format(pathdir))
    sdpsql=ReadTxtFile('{0}/sql/sdptraffictoday.sql'.format(pathdir))
    scpsql=ReadTxtFile('{0}/sql/scptraffictoday.sql'.format(pathdir))
    sdpminute=ReadTxtFile('{0}/sql/sdptrafficminute.sql'.format(pathdir))
    scpminute=ReadTxtFile('{0}/sql/scptrafficminute.sql'.format(pathdir))
    try :
        datascp=GetPandasToday(conpath=scpcon,sqlraw=scpsql,tgl=today)
        datasdp=GetPandasToday(conpath=sdpcon,sqlraw=sdpsql,tgl=today)
        datascp.to_csv('rawdata/scp_today_hourly.csv',index=False)
        datasdp.to_csv('rawdata/sdp_today_hourly.csv',index=False)
        print('grep new data hourly success !!!')
        sqlscpmin=scpminute.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
        sqlsdpmin=sdpminute.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
        conscp=OracleCon(scpcon)
        scpmin=pd.read_sql(sqlscpmin,con=conscp)
        consdp=OracleCon(sdpcon)
        sdpmin=pd.read_sql(sqlsdpmin,con=consdp)
        scpmin.to_csv('rawdata/scp_today_minute.csv',index=False)
        sdpmin.to_csv('rawdata/sdp_today_minute.csv',index=False)
        print('grep new data minute success !!!')
    except Exception :
        print('grep new data minute failed !!!')

while True :
    GrepNewData()
    time.sleep(300)