import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list


def GetDataUpdate(env=None):
    #variable
    pathdir=os.getcwd()
    today=GetToday()
    dt1=today - timedelta(minutes=8)
    dt2=today - timedelta(minutes=1)
    mon=ConvertDatetoStr(tgl=dt1,format='%m')
    day=ConvertDatetoStr(tgl=dt1,format='%d')
    tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
    tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
    if str(env).lower() == 'scp' :
        print('get data scp starting ...') 
        conpath=(f'{pathdir}/connections/scpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/scptrafficupdate.sql')
        output=f'{pathdir}/rawdata/scp_data_raw.csv'
        list_column=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL']
    else :
        print('get data sdp starting ...') 
        conpath=(f'{pathdir}/connections/sdpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/sdptrafficupdate.sql')
        output=f'{pathdir}/rawdata/sdp_data_raw.csv'
        list_column=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']
    cred=ReadJsonFile(conpath)
    string_connection=f'{cred["username"]}/{cred["password"]}@{cred["host"]}:{cred["port"]}/{cred["sid"]}'
    sql=sqltxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    sqlcmd=f"""/usr/lib/oracle/18.3/client64/bin/sqlplus -s {string_connection} <<EOF
SET MARKUP CSV ON

SPOOL{output}

{sql};

SPOOL OFF
EXIT
EOF
"""
    os.popen(sqlcmd)
 

while True :
    GetDataUpdate(env='scp')
    GetDataUpdate(env='sdp')
    time.sleep(120)

