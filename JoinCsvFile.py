# -*- coding: utf-8 -*-
"""
Created on Wed Nov  1 19:41:32 2023

@author: HS00935501
"""

import pandas as pd
import os
import json
import time
from datetime import datetime,timedelta
from modules.general import GetToday



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
                cond=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['SERVICE_KEY']==dn[1]['SERVICE_KEY']) & (dfold['DIAMETER']==dn[1]['DIAMETER'])
                dfold.loc[(cond),['TOTAL']]=dn[1]['TOTAL']
            else :
                cond=(dfold['CDRDATE']==dn[1]['CDRDATE']) & (dfold['CP_NAME']==dn[1]['CP_NAME'])  & (dfold['ACCESSFLAG']==dn[1]['ACCESSFLAG']) & (dfold['BASICCAUSE']==dn[1]['BASICCAUSE']) & (dfold['INTERNALCAUSE']==dn[1]['INTERNALCAUSE'])
                dfold.loc[(cond),['REVENUE']]=dn[1]['REVENUE']
                dfold.loc[(cond),['TOTAL']]=dn[1]['TOTAL']
        dfnewfilter=dfnew[~dfnew['CDRDATE'].isin(dfold['CDRDATE'].tolist())]
        dfraw=pd.concat([dfold[listcolumn],dfnewfilter[listcolumn]],ignore_index=True).reset_index()
    else :
        dfraw=dfnew
    dfrawfilter=dfraw[dfraw['CDRDATE'].str.contains('rows selected') == False]
    dfrawfilter['CDRDATE2']=pd.to_datetime(dfrawfilter['CDRDATE'], format='%Y-%m-%d %H:%M')
    dfrawfilter['DATE']=dfrawfilter['CDRDATE2'].dt.date
    dffilterfinal=dfrawfilter[dfrawfilter['DATE'] >= d1.date() ]
    dffilterfinal[listcolumn].to_csv(outputfile,index=False)
    print('data  wrap to file done !!!')
    

olddatascp='rawdata/data_scp_today.csv'
newdatascp='rawdata/scp_data_raw.csv'
olddatasdp='rawdata/data_sdp_today.csv'
newdatasdp='rawdata/sdp_data_raw.csv'
scpcolumn=['CDRDATE','SERVICE_KEY','IS_ROAMING','DIAMETER','TOTAL']
sdpcolumn=['CDRDATE','CP_NAME','CPID','ACCESSFLAG','BASICCAUSE','INTERNALCAUSE','REVENUE','TOTAL']

while True :
    JoinCsvFile(oldfile=olddatascp,newfile=newdatascp,env='SCP',listcolumn=scpcolumn,outputfile=olddatascp)
    JoinCsvFile(oldfile=olddatasdp,newfile=newdatasdp,env='sdp',listcolumn=sdpcolumn,outputfile=olddatasdp)
    time.sleep(240)