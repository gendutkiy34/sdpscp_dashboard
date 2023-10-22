import time
import pandas as pd
import time
import os
from datetime import timedelta
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.connection import OracleCon



def GetDataNow():
    #variable
    pathdir=os.getcwd()
    today=GetToday()
    dt1=today - timedelta(minutes=5)
    dt2=today - timedelta(minutes=1)
    mon=ConvertDatetoStr(tgl=today,format='%m')
    day=ConvertDatetoStr(tgl=today,format='%d')
    tm1=ConvertDatetoStr(tgl=dt1,format='%H:%M')
    tm2=ConvertDatetoStr(tgl=dt2,format='%H:%M') 
    scpcon=(f'{pathdir}/connections/scpprodtrx.json')
    sdpcon=(f'{pathdir}/connections/sdpprodtrx.json')
    sdptxt=ReadTxtFile(f'{pathdir}/sql/sdptrafficupdate.sql')
    scptxt=ReadTxtFile(f'{pathdir}/sql/scptrafficupdate.sql')
    outputsdp=f'{pathdir}/rawdata/sdp_data_raw.csv'
    outputscp=f'{pathdir}/rawdata/scp_data_raw.csv'
    scpsql=scptxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    sdpsql=sdptxt.format(day=day,mon=mon,tm1=tm1,tm2=tm2)
    list1=[]
    #scpdata
    try :
        dfscpold=pd.read_csv(outputscp)
        conn=OracleCon(scpcon)
        if conn != "connection failed !!!!"  :
            dfnew=pd.read_sql(scpsql, con=conn)
        else :
            dfnew=pd.DataFrame(list1)
        dftemp=dfnew[~dfnew['CDRDATE'].isin(dfscpold['CDRDATE'].tolist())]
        dfscpraw=pd.concat([dfscpold[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']],dftemp[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']]], ignore_index=True).reset_index()
    except Exception :
        try :
            conn=OracleCon(scpcon)
            if conn != "connection failed !!!!"  :
                dfnew=pd.read_sql(scpsql, con=conn)
            else :
                dfnew=pd.DataFrame(list1)
            dfscpraw=dfnew
        except Exception :
            dfscpraw=pd.DataFrame(list1)
        dfscpold=pd.DataFrame(list1)  
    print(f'data scp --> old : {len(dfscpold.index)},new : {len(dfnew.index)}')
    dfscpraw[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']].iloc[-50000:].to_csv(outputscp,index=False)
    print('data SCP wrap to file done !!!')
    try :
        dfsdpold=pd.read_csv(outputsdp)
        conn=OracleCon(sdpcon)
        if conn != "connection failed !!!!"  :
            dfnew=pd.read_sql(sdpsql, con=conn)
        else :
            dfnew=pd.DataFrame(list1)
        dftemp=dfnew[~dfnew['CDRDATE'].isin(dfsdpold['CDRDATE'].tolist())]
        dfsdpraw=pd.concat([dfsdpold[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']],dftemp[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']]], ignore_index=True).reset_index()
    except Exception :
        try :
            conn=OracleCon(sdpcon)
            if conn != "connection failed !!!!"  :
                dfnew=pd.read_sql(sdpsql, con=conn)
            else :
                dfnew=pd.DataFrame(list1)
            dfsdpraw=dfnew
        except Exception :
            dfsdpraw=pd.DataFrame(list1)
        dfsdpold=pd.DataFrame(list1)  
    print(f'data sdp --> old : {len(dfsdpold.index)},new : {len(dfnew.index)}')
    dfsdpraw[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']].iloc[-50000:].to_csv(outputsdp,index=False)
    print('data SDP wrap to file done !!!')



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
        
    '''if i > 1 :
                dfscpraw=pd.concat([dftmp[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']],dfnew[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']]], ignore_index=True).reset_index()
        else :
                dftmp=dfnew
        print(f'Get data scp day {i}')
        try :
            conn=OracleCon(scpcon)
            if conn != "connection failed !!!!"  :
                dfnew=pd.read_sql(scpsql, con=conn)
            else :
                dfnew=pd.DataFrame(list1)
            if i > 1 :
                dfscpraw=pd.concat([dftmp[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']],dfnew[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']]], ignore_index=True).reset_index()
            else :
                dftmp=dfnew
            print(f'Get data scp day {i}')
        except Exception :
            dfscpraw=pd.DataFrame(list1)
        i += 1
    #print(f'Grep SCP data done, total data : {len(dfscpraw.index)}')
    #dfscpraw[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']].to_csv(outputscp,index=False)
    #print('data SCP wrap to file done !!!')
i=1
    for dt in list_day:
        mon=ConvertDatetoStr(tgl=dt,format='%m')
        day=ConvertDatetoStr(tgl=dt,format='%d')
        sdpsql=sdptxt.format(day=day,mon=mon,hourmin=hourmin)
        try :
            conn=OracleCon(sdpcon)
            if conn != "connection failed !!!!"  :
                dfnew=pd.read_sql(sdpsql, con=conn)
            else :
                dfnew=pd.DataFrame(list1)
            if i > 1 :
                dfsdpraw=pd.concat([dftmp[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']],dfnew[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']]], ignore_index=True).reset_index()
            else :
                dftmp=dfnew
            print(f'Get data sdp day {i}')
        except Exception :
            dfsdpraw=pd.DataFrame(list1)
        i += 1
    print(f'Grep SDP data done, total data : {len(dfsdpraw.index)}')
    dfsdpraw[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']].to_csv(outputsdp,index=False)
    print('data SCP wrap to file done !!!')'''
        
    