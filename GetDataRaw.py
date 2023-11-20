import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list


pd.options.mode.chained_assignment = None

#GET DATA Now
def GetDataUpdate(env=None):
    #variable
    pathdir=os.getcwd()
    today=GetToday()
    dt1=today - timedelta(minutes=10)
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


#GET DATA D0 ,D-1, D-7
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
        list_column=['CDRDATE','SERVICE_KEY','IS_ROAMING','ISBFT','DIAMETER','TOTAL','REMARK']
        tmpcolumn=['CDRDATE','SERVICE_KEY','IS_ROAMING','ISBFT','DIAMETER','TOTAL']
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
    dfoutput=pd.DataFrame(listraw,columns=list_column)
    dfoutput[list_column].to_csv(output,index=False)
    print('wrap to file done !!!')


#Join Data Now and before minute
def JoinCsvFile(oldfile=None,newfile=None,listcolumn=None,env=None,outputfile=None):
    listempty=[]
    today=GetToday()
    d1=today-timedelta(days=1)
    try :
        dfold=pd.read_csv(oldfile)
    except Exception :
        dfold=pd.DataFrame(listempty,columns=listcolumn)
    try :
        dfnew=pd.read_csv(newfile)
    except Exception :
        dfnew=pd.DataFrame(listempty,columns=listcolumn)
    if len(dfold.index) > 0 :
        for dn in dfnew.iterrows():
            if env.lower() == 'scp' :
                cond=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['SERVICE_KEY']==dn[1]['SERVICE_KEY']) & (dfold['DIAMETER']==dn[1]['DIAMETER']) & (dfold['TOTAL'] == '0' )
                dfold.loc[(cond),['TOTAL']]=dn[1]['TOTAL']
            else :
                cond1=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['CP_NAME']==dn[1]['CP_NAME'])  & (dfold['ACCESSFLAG']==dn[1]['ACCESSFLAG']) & (dfold['BASICCAUSE']==dn[1]['BASICCAUSE']) & (dfold['INTERNALCAUSE']==dn[1]['INTERNALCAUSE']) & (dfold['TOTAL'] == '0' )  
                cond2=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['CP_NAME']==dn[1]['CP_NAME'])  & (dfold['ACCESSFLAG']==dn[1]['ACCESSFLAG']) & (dfold['BASICCAUSE']==dn[1]['BASICCAUSE']) & (dfold['INTERNALCAUSE']==dn[1]['INTERNALCAUSE']) & (dfold['REVENUE'] == '0' )
                dfold.loc[(cond1),['TOTAL']]=dn[1]['TOTAL']
                dfold.loc[(cond2),['REVENUE']]=dn[1]['REVENUE']
        try :
            dfnewfilter=dfnew[~dfnew['CDRDATE'].isin(dfold['CDRDATE'].tolist())]
        except Exception :
            dfnewfilter=pd.DataFrame(listempty,columns=listcolumn)
        dfraw=pd.concat([dfold[listcolumn],dfnewfilter[listcolumn]],ignore_index=True).reset_index()
    else :
        dfraw=dfnew
    dfrawfilter=dfraw[dfraw['CDRDATE'].str.contains('rows selected') == False]
    dfrawfilter['CDRDATE2']=pd.to_datetime(dfrawfilter['CDRDATE'], format='%Y-%m-%d %H:%M')
    dfrawfilter['DATE']=dfrawfilter['CDRDATE2'].dt.date
    dffilterfinal=dfrawfilter.iloc[-75000:]
    dffilterfinal[listcolumn].to_csv(outputfile,index=False)
    print('data  wrap to file done !!!')


#VARIABLE
olddatascp='rawdata/data_scp_today.csv'
newdatascp='rawdata/scp_data_raw.csv'
olddatasdp='rawdata/data_sdp_today.csv'
newdatasdp='rawdata/sdp_data_raw.csv'
scpcolumn=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL']
sdpcolumn=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']


while True :
    GetDataUpdate(env='scp')
    JoinCsvFile(oldfile=olddatascp,newfile=newdatascp,env='SCP',listcolumn=scpcolumn,outputfile=olddatascp)
    GetDataUpdate(env='sdp')
    JoinCsvFile(oldfile=olddatasdp,newfile=newdatasdp,env='sdp',listcolumn=sdpcolumn,outputfile=olddatasdp)
    GetDataD017(env='scp')    
    GetDataD017(env='sdp')
    time.sleep(60)
