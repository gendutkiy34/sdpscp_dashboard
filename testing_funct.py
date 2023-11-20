import pandas as pd
import os
import json
import time
from datetime import datetime,timedelta
from modules.general import GetToday


pd.options.mode.chained_assignment = None

def JoinCsvFile(oldfile=None,newfile=None,listcolumn=None,env=None,outputfile=None):
    listempty=[]
    today=GetToday()
    d1=today-timedelta(days=1)
    dfold=pd.read_csv(oldfile)
    print(dfold.tail())
    dfnew=pd.read_csv(newfile)
    print(dfnew.head())
    if len(dfold.index) > 0 :
        for dn in dfnew.iterrows():
            if env.lower() == 'scp' :
                cond=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['SERVICE_KEY']==dn[1]['SERVICE_KEY']) & (dfold['DIAMETER']==dn[1]['DIAMETER']) & (dfold['TOTAL'] == '0' )
                dfold.loc[(cond),['TOTAL']]=dn[1]['TOTAL']
            else :
                #cond1=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['CP_NAME']==dn[1]['CP_NAME'])  & (dfold['ACCESSFLAG']==dn[1]['ACCESSFLAG']) & (dfold['BASICCAUSE']==dn[1]['BASICCAUSE']) & (dfold['INTERNALCAUSE']==dn[1]['INTERNALCAUSE']) & (dfold['TOTAL'] == '0' )
                #cond2=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['CP_NAME']==dn[1]['CP_NAME'])  & (dfold['ACCESSFLAG']==dn[1]['ACCESSFLAG']) & (dfold['BASICCAUSE']==dn[1]['BASICCAUSE']) & (dfold['INTERNALCAUSE']==dn[1]['INTERNALCAUSE']) & (dfold['REVENUE'] == '0' )
                cond1=(dfold.CDRDATE==dn[1]['CDRDATE']) & (dfold.CP_NAME==dn[1]['CP_NAME'])  & (dfold.ACCESSFLAG==dn[1]['ACCESSFLAG']) & (dfold.BASICCAUSE==dn[1]['BASICCAUSE']) & (dfold.INTERNALCAUSE==dn[1]['INTERNALCAUSE']) & (dfold.TOTAL== '0' )
                cond2=(dfold.CDRDATE==dn[1]['CDRDATE']) & (dfold.CP_NAME==dn[1]['CP_NAME'])  & (dfold.ACCESSFLAG==dn[1]['ACCESSFLAG']) & (dfold.BASICCAUSE==dn[1]['BASICCAUSE']) & (dfold.INTERNALCAUSE==dn[1]['INTERNALCAUSE']) & (dfold.REVENUE == '0' )
                dfold.loc[(cond1),'TOTAL']=dn[1]['TOTAL']
                dfold.loc[(cond2),'REVENUE']=dn[1]['REVENUE']
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
    print(dffilterfinal.head(10))
    #dffilterfinal[listcolumn].to_csv(outputfile,index=False)
    print('data  wrap to file done !!!')
    

olddatascp='rawdata/data_scp_today.csv'
newdatascp='rawdata/scp_data_raw.csv'
olddatasdp='rawdata/data_sdp_today.csv'
newdatasdp='rawdata/sdp_data_raw.csv'
scpcolumn=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL']
sdpcolumn=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']

JoinCsvFile(oldfile=olddatasdp,newfile=newdatasdp,env='sdp',listcolumn=sdpcolumn,outputfile=olddatasdp)

