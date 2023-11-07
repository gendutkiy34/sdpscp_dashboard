import os
import pandas as pd
import time
from datetime import datetime
from modules.general import GetToday
from modules.connection import OracleCon
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017



rawminscp='./rawdata/data_scp_today.csv'
rawminsdp='./rawdata/data_sdp_today.csv'
rawd3scp='./rawdata/scp_data_d017.csv'
rawd3sdp='./rawdata/sdp_data_d017.csv'
listsr=[66,67,68,72,73]
datascp=ScpDataD017(rawd3scp)
scpatt,scpsuc,scpsr,listdia,lisum=datascp.SumDataToday()
print(lisum)


'''    dic_data={}
    for s in listsr :
        att_label=f'att{s}'
        suc_label=f'suc{s}'
        sr_label=f'sr{s}'
        att,succ,hour=datasdp.AttHourToday(accflag=s)
        sr=datasdp.SrHourToday(accflag=s)
        print(sr)
        dic_data[att_label]=att
        dic_data[suc_label]=succ    
        dic_data[sr_label]=sr 
    list_hour=[]
    for h in hour :
        if len(str(h)) < 2:
            list_hour.append(f'0{h}')
        else :
            list_hour.append(h)
    dic_data['hour']=list_hour
    cprev,rev=datasdp.RevTop5()
    topcp['cprev']=cprev
    topcp['revenue']=rev
    cpatt,att=datasdp.AttTop5()
    topcp['cpatt']=cpatt
    topcp['attempt']=att
    dfsum=datasdp.SummaryToday()
    dic_data['summary']=dfsum
    print(dic_data)



list_rev=[941,949,938]
today=GetToday()
dataraw=pd.read_csv(rawscp)
dataraw=dataraw.fillna(0)
dataraw['CDRDATE2']=pd.to_datetime(dataraw['CDRDATE'], format='%Y-%m-%d %H:%M')
dataraw['TOTAL']=dataraw['TOTAL'].astype(int)
dataraw['REVENUE']=dataraw['REVENUE'].astype(int)
dataraw['INTERNALCAUSE']=dataraw['INTERNALCAUSE'].astype(int)
dataraw['BASICCAUSE']=dataraw['BASICCAUSE'].astype(int)
dataraw['ACCESSFLAG']=dataraw['ACCESSFLAG'].astype(int)
dataraw['CPID']=dataraw['CPID'].astype(int)
dataraw['DATE']=dataraw['CDRDATE2'].dt.date
dataraw['HOUR']=dataraw['CDRDATE2'].dt.hour
df_sdp_today=dataraw[dataraw['DATE'] == today.date()]
basiccause_suc=[852,963,123,949,938,941]
dffilter=df_sdp_today[df_sdp_today['INTERNALCAUSE'].isin(list_diameter)]
#dffilter=df_sdp_today[(df_sdp_today['INTERNALCAUSE']==2001)]
dffilter=dffilter[['HOUR','ACCESSFLAG','TOTAL']]
dfsuc=dffilter.groupby(['HOUR','ACCESSFLAG'])['TOTAL'].sum('TOTAL').reset_index()
dfsuc=dfsuc.rename(columns={'TOTAL':'SUCCESS'})
dfatt = df_sdp_today.groupby(['HOUR','ACCESSFLAG'])['TOTAL'].sum('TOTAL').reset_index()
dfatt=dfatt.rename(columns={'TOTAL':'ATTEMPT'})
dfjoin=pd.merge(dfatt,dfsuc,on=['HOUR','ACCESSFLAG'])
dfjoin['SR']=dfjoin.apply(lambda x : round(x['SUCCESS']/x['ATTEMPT']*100,2) ,axis=1)
dfcp=df_sdp_today[['CP_NAME','ACCESSFLAG','TOTAL']]
dfsum = dfcp.groupby('CP_NAME')['TOTAL'].sum('TOTAL').reset_index()
dfsort=dfsum.sort_values('TOTAL',ascending=False).head(5)
dfacc = dfcp.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
dfacc=dfacc.rename(columns={'TOTAL':'ATTEMPT'})
dfsucacc=dffilter.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
dfsucacc=dfsucacc.rename(columns={'TOTAL':'SUCCESS'})
rawrev=df_sdp_today[(df_sdp_today['INTERNALCAUSE']==2001) & (df_sdp_today['BASICCAUSE'].isin(list_rev))]
dfrev=rawrev.groupby('ACCESSFLAG')['REVENUE'].sum('REVENUE').reset_index()
dfjoin1=pd.merge(dfacc[['ACCESSFLAG','ATTEMPT']],dfsucacc[['ACCESSFLAG','SUCCESS']],on=['ACCESSFLAG']).reset_index()
dffinaljoin=pd.merge(dfjoin1[['ACCESSFLAG','ATTEMPT','SUCCESS']],dfrev[['ACCESSFLAG','REVENUE']],on=['ACCESSFLAG']).reset_index()
list_hour=dataraw['HOUR'].drop_duplicates().tolist()
#list_rev=[941,949,938]
#df_rev=dataraw[(dataraw['INTERNALCAUSE']==2001) & (dataraw['BASICCAUSE'].isin(list_rev))]
#rev_final=pd.pivot_table(df_rev,values='REVENUE', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
print(list_hour)
#list0,list1,list7,listh=datascp.AttRoam(roaming=1)
#print(datatoday.info())
#print(list0)
#print(list1)
#print(list7)
#print(listh)'''







