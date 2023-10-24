import time
import pandas as pd
import time
import os
from datetime import timedelta
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.connection import OracleCon



def GetDataNow(env=None):
    #variable
    pathdir=os.getcwd()
    today=GetToday()
    dt1=today - timedelta(minutes=5)
    dt2=today - timedelta(minutes=1)
    mon=ConvertDatetoStr(tgl=today,format='%m')
    day=ConvertDatetoStr(tgl=today,format='%d')
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
        list_column=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']
    sql=sqltxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    list1=[]
    try :
        dfold=pd.read_csv(output)
        conn=OracleCon(conpath)
        if conn != "connection failed !!!!"  :
            dfnew=pd.read_sql(sql, con=conn)
        else :
            dfnew=pd.DataFrame(list1)
        dftemp=dfnew[~dfnew['CDRDATE'].isin(dfold['CDRDATE'].tolist())]
        dfraw=pd.concat([dfold[list_column],dftemp[list_column]], ignore_index=True).reset_index()
    except Exception :
        try :
            conn=OracleCon(conpath)
            if conn != "connection failed !!!!"  :
                dfraw=pd.read_sql(sql, con=conn)
            else :
                dfraw=pd.DataFrame(list1)
        except Exception :
            dfraw=pd.DataFrame(list1)
        dfold=pd.DataFrame(list1)  
    print(len(dfraw.index))   
    #dfraw[list_column].iloc[-50000:].to_csv(output,index=False)
    #print('data SCP wrap to file done !!!')
    


def GetDataNew(env=None):
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
    sql=sqltxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    list1=[]
    try :
        dfold=pd.read_csv(output)
        conn=OracleCon(conpath)
        if conn != "connection failed !!!!"  :
            cur=conn.cursor()
            tempdata=cur.execute(sql)
        else :
            tempdata=list1
        list_raw=[]
        [list_raw.append(d) for d in tempdata]
        if len(list_raw) > 0 :
            dfnew=pd.DataFrame(list_raw,columns=list_column)
            dftemp=dfnew[~dfnew['CDRDATE'].isin(dfold['CDRDATE'].tolist())]
            dfraw=pd.concat([dfold[list_column],dftemp[list_column]], ignore_index=True).reset_index()
        else :
            dfraw=pd.DataFrame(list1)
        print(tempdata)
    except Exception :
        try :
            conn=OracleCon(conpath)
            if conn != "connection failed !!!!"  :
                cur=conn.cursor()
                tempdata=cur.execute(sql)
            else :
                tempdata=list1
            list_raw=[]
            [list_raw.append(d) for d in tempdata]
            dfraw=pd.DataFrame(list_raw,columns=list_column)
        except Exception :
            dfraw=pd.DataFrame(list1)
        dfold=pd.DataFrame(list1) 
    dfraw[list_column].iloc[-50000:].to_csv(output,index=False)
    print('data SCP wrap to file done !!!')


def GetData3Days(day0=None,env=None):
    pathdir=os.getcwd()
    today=GetToday()
    todaystr=ConvertDatetoStr(today,format='%Y-%m-%d')
    day0str=ConvertDatetoStr(day0,format='%Y-%m-%d')
    if day0str == todaystr :
        hourmin=ConvertDatetoStr(tgl=today,format='%H:%M')
    else :
        hourmin='23:59'
    list_day=[0,1,7]
    if str(env).lower() == 'scp' :
        print('get data scp starting ...') 
        conpath=(f'{pathdir}/connections/scpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/scptraffic3day.sql')
        output=f'{pathdir}/rawdata/scp_data_3day.csv'
        list_column=['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL','REMARK']
    else :
        print('get data sdp starting ...') 
        conpath=(f'{pathdir}/connections/sdpprodtrx.json')
        sqltxt=ReadTxtFile(f'{pathdir}/sql/sdptraffic3day.sql')
        output=f'{pathdir}/rawdata/sdp_data_3day.csv' 
        list_column=['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL','REMARK']
    i=1
    list1=[]
    list_raw=[]
    conn=OracleCon(conpath)
    for n in list_day:
        dt=day0 - timedelta(days=n)
        remark=f'day{n}'
        mon=ConvertDatetoStr(tgl=dt,format='%m')
        day=ConvertDatetoStr(tgl=dt,format='%d')
        sql=sqltxt.format(day=day,mon=mon,hourmin=hourmin,remarkday=remark)
        if conn != "connection failed !!!!"  :
                dfnew=pd.read_sql(sql, con=conn)
                dfnew['REMARK']=remark
        else :
                dfnew=pd.DataFrame(list1)
        listtmp=dfnew[list_column].values.tolist()
        [list_raw.append(t) for t in listtmp]
        print(f'Get data day {n} done !!!')
    dfraw=pd.DataFrame(list_raw,columns=list_column)
    dfraw[list_column].to_csv(output,index=False)
    print('data SCP wrap to file done !!!')    