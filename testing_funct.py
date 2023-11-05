import os
import pandas as pd
import time
from datetime import datetime
from modules.connection import OracleCon
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017


#path=os.path.abspath(os.path.dirname(__file__))
#print(path)
pathdir=os.getcwd()
rawscp=f'{pathdir}/rawdata/sdp_data_d017.csv'

datascp=SdpDataD017(rawscp)
datatoday=datascp.VerifyDataRaw()
list0,list1,list7,listh=datascp.Revenue()
print(datatoday.head())
print(list0)
print(list1)
print(list7)
print(listh)
'''dataraw=dataraw.fillna(0)
dataraw['CDRDATE2']=pd.to_datetime(dataraw['CDRDATE'], format='%Y-%m-%d %H')
dataraw['TOTAL']=dataraw['TOTAL'].astype(int)
dataraw['REVENUE']=dataraw['REVENUE'].astype(int)
dataraw['CPID']=dataraw['CPID'].astype(int)
dataraw['DATE']=dataraw['CDRDATE2'].dt.date
dataraw['HOUR']=dataraw['CDRDATE2'].dt.hour
dffilter=dataraw[['ACCESSFLAG','INTERNALCAUSE','BASICCAUSE']]
dffilter['INTERNALCAUSE']=dffilter['INTERNALCAUSE'].astype(int)
dffilter['ACCESSFLAG']=dffilter['ACCESSFLAG'].astype(int)
dfsuc=dffilter[dffilter['INTERNALCAUSE']==2001].drop_duplicates()
list_rev=[941,949,938]
df_rev=dataraw[(dataraw['INTERNALCAUSE']==2001) & (dataraw['BASICCAUSE'].isin(list_rev))]
rev_final=pd.pivot_table(df_rev,values='REVENUE', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
print(rev_final)
#list0,list1,list7,listh=datascp.AttRoam(roaming=1)
#print(datatoday.info())
#print(list0)
#print(list1)
#print(list7)
#print(listh)'''







