import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list


#grep data minute
def GetDataMinute(pathsql=None,pathconnection=None,dat=None):
    if pathsql is not None and pathconnection is not None  and dat is not None :  #verify argument is not null
        dt1=dat - timedelta(minutes=6) 
        dt2=dat - timedelta(minutes=1)
        mon=ConvertDatetoStr(tgl=dt1,format='%m')
        day=ConvertDatetoStr(tgl=dt1,format='%d')
        tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
        tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M')
        conpath=pathconnection
        sqltxt=pathsql
        print('get data starting ...')
        cred=ReadJsonFile(conpath)
        string_connection=f'{cred["username"]}/{cred["password"]}@{cred["host"]}:{cred["port"]}/{cred["sid"]}'
        sql=sqltxt.format(day=day,mon=mon,min1=tm1,min2=tm2)
        sqlcmd=f"""export ORACLE_HOME=/usr/lib/oracle/18.3/client64
export PATH=$ORACLE_HOME/bin:$ORACLE_HOME/OPatch:$PATH
sqlplus -s {string_connection} <<EOF
SET MARKUP CSV ON



{sql};

SPOOL OFF
EXIT
EOF
"""
        temp_data=os.popen(sqlcmd)
        list_raw=[]
        i=0
        for d in temp_data :
            temp=d.replace('"','').replace('\n','').split(',')
            if int(len(temp)) > 1 :
                if i < 1 :
                    list_col=temp
                else :
                    list_raw.append(temp)
                i += 1
            else :
                pass
        df=pd.DataFrame(list_raw,columns=list_col)
        temp_data=df.to_dict('records')
    else : #verify argument is not null
        temp_data=None
    return temp_data


#grep data houe
def GetDataHour(pathsql=None,pathconnection=None,dat=None):
    if pathsql is not None and pathconnection is not None and dat is not None: #verify argument is not null
        mon=ConvertDatetoStr(tgl=dat,format='%m')
        day=ConvertDatetoStr(tgl=dat,format='%d')
        tm=ConvertDatetoStr(tgl=dat,format='%H:%M')
        conpath=pathconnection
        sqltxt=pathsql
        print('get data starting ...')
        cred=ReadJsonFile(conpath)
        string_connection=f'{cred["username"]}/{cred["password"]}@{cred["host"]}:{cred["port"]}/{cred["sid"]}'
        sql=sqltxt.format(day=day,mon=mon,min=tm)
        #/usr/lib/oracle/18.3/client64/bin/sqlplus
        sqlcmd=f"""export ORACLE_HOME=/usr/lib/oracle/18.3/client64
export PATH=$ORACLE_HOME/bin:$ORACLE_HOME/OPatch:$PATH
sqlplus -s {string_connection} <<EOF
SET MARKUP CSV ON



{sql};

SPOOL OFF
EXIT
EOF
"""
        temp_data=os.popen(sqlcmd)
        list_raw=[]
        list_col=[]
        i=0
        for d in temp_data :
            temp=d.replace('"','').replace('\n','').split(',')
            if int(len(temp)) > 1 :
                if i < 1 :
                    list_col=temp
                else :
                    list_raw.append(temp)
                i += 1
            else :
                pass
        df=pd.DataFrame(list_raw,columns=list_col)
        temp_data=df.to_dict('records')
    else : #verify argument is not null
        temp_data=None
    return temp_data

