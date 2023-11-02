# -*- coding: utf-8 -*-
"""
Created on Thu Nov  2 10:59:57 2023

@author: HS00935501
"""

import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list


def GetDataD017(env=None):
    #variable
    pathdir=os.getcwd()
    today=GetToday()
    hourmin=today.strftime('%H:%M')
    list_n=[0,1,7]
    if str(env).lower() == 'scp' :
        print('get data scp starting ...') 
        conpath=(f'{pathdir}/connections/scpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/scptraffic3day.sql')
        tempoutput=f'{pathdir}/rawdata/scp_data_temp.csv'
        output=f'{pathdir}/rawdata/scp_data_d017.csv'
        list_column=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL','REMARK']
        tmpcolumn=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL']
    else :
        print('get data sdp starting ...') 
        conpath=(f'{pathdir}/connections/sdpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/sdptraffic3day.sql')
        tempoutput=f'{pathdir}/rawdata/sdp_data_temp.csv'
        output=f'{pathdir}/rawdata/sdp_data_d017.csv'
        list_column=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL','REMARK']
        tmpcolumn=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']
    cred=ReadJsonFile(conpath)
    string_connection=f'{cred["username"]}/{cred["password"]}@{cred["host"]}:{cred["port"]}/{cred["sid"]}'
    listraw=[]
    for n in list_n :
        dt=today - timedelta(days=n)
        dtstring=ConvertDatetoStr(tgl=dt,format='%Y-%m-%d')
        remark=f'day{n}'
        mon=ConvertDatetoStr(tgl=dt,format='%m')
        day=ConvertDatetoStr(tgl=dt,format='%d')
        sql=sqltxt.format(day=day,mon=mon,hourmin=hourmin)
        sqlcmd=f"""/usr/lib/oracle/18.3/client64/bin/sqlplus -s {string_connection} <<EOF
SET MARKUP CSV ON


{sql};

SPOOL OFF
EXIT
EOF"""
        temp=os.popen(sqlcmd)
        for t in temp :
            if 'rows selected' in t or 'spooling'in t or 'CDRDATE' in t or t is None :
                pass
            else :
                if dtstring in t :
                    t2=t.replace('\n','').replace('"','')
                    tempremark=f'{t2},{remark}'
                    temptxt=tempremark.split(',')
                    listraw.append(temptxt)
                else :
                    pass
        print(f'get data day {n} done')
    dfoutput=pd.DataFrame(listraw)
    print(dfoutput.head())
    dfoutput=pd.DataFrame(listraw,columns=list_column)
    dfoutput[list_column].to_csv(output,index=False)
    print('wrap to file done !!!')
    

while True :
    GetDataD017(env='scp')    
    GetDataD017(env='sdp')
    time.sleep(500)
