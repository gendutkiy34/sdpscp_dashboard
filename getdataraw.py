import time
from datetime import timedelta
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.connection import OracleCon
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import pandas as pd


def GrepNewData():
    today=GetToday()
    dt1=today - timedelta(minutes=22)
    dt2=today - timedelta(minutes=2)
    mon=ConvertDatetoStr(tgl=today,format='%m')
    day=ConvertDatetoStr(tgl=today,format='%d')
    tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
    tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
    print(tm1,tm2)
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