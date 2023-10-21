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
    dfscpraw[['CDRDATE','SERVICE_KEY','DIAMETER','TOTAL']].iloc[-50000:].to_csv(outputscp,index=False)
    dfsdpraw[['CDRDATE','CP_NAME','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','TOTAL']].iloc[-50000:].to_csv(outputsdp,index=False)
    print(f'wrap to csv file done !!!')
        
        
    