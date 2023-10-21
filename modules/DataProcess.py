import pandas as pd
import os
import time 
from datetime import datetime,timedelta
from modules.general import GetToday

list_accflag=['66','67','68','72','73']
list_diameter=[2001,4012,4010,5031,5030]
list_errcode={"66":[940,938],
              "67":[940,941,83,81],
              "68":[111,949,948],
              "72":[372,987,123,940,940],
              "73":[111,940,941,83,23,991]}
list_sk=[100,200,133,150,300,400]



class ScpData():


    def __init__(self,pathfile=None):
        try : 
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'])
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].fillna(0)
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception:
            self.flagdata=0
        #return self.flagdata


    def SumDataToday(self):
        if self.flagdata > 0 :
            today=GetToday()
            self.df_scp_today=self.dataraw[self.dataraw['DATE']== today.date()]
            self.dfscpsuc=self.df_scp_today[self.df_scp_today['DIAMETER'].isin(list_diameter)]
            scpatt=pd.Series(self.df_scp_today['TOTAL']).sum()
            scpsuc=pd.Series(self.dfscpsuc['TOTAL']).sum()
            scpsr=round((scpsuc/scpatt)*100,2)
        else :
            scpatt='N/A'
            scpsuc='N/A'
            scpsr='N/A'
        return scpatt,scpsuc,scpsr


    def HourlyDataToday(self):
        if self.flagdata > 0 :
            list_hour=self.df_scp_today['HOUR'].drop_duplicates().tolist()
            dfhourlyatt=self.df_scp_today[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            dfhourlysuc=self.dfscpsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
        else :
            dfhourlyatt=[]
            dfhourlysuc=[]
            list_hour=[]
        return dfhourlyatt['TOTAL'].tolist(),dfhourlysuc['TOTAL'].tolist(),list_hour
        




class SdpData():

    def __init__(self,pathfile=None):
        try :
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'])
            self.dataraw['INTERNALCAUSE']=self.dataraw['INTERNALCAUSE'].fillna(0)
            self.dataraw['INTERNALCAUSE ']=self.dataraw['INTERNALCAUSE'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception :
            self.flagdata=0
    
    def SumDataToday(self,accflag=None):
        if self.flagdata > 0:
            self.today=GetToday()
            self.df_sdp_today=self.dataraw[self.dataraw['DATE']== self.today.date()]
            rawatt=self.df_sdp_today[self.df_sdp_today['ACCESSFLAG']==int(accflag)]
            rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
            sdpatt=pd.Series(rawatt['TOTAL']).sum()
            sdpsuc=pd.Series(rawsuc['TOTAL']).sum()
            sdpsr=round((sdpsuc/sdpatt)*100,2)
        else :
            sdpatt='N/A'
            sdpsuc='N/A'
            sdpsr='N/A'
        return sdpatt,sdpsuc,sdpsr
    

    def HourlyDataToday(self,accflag=None):
        if self.flagdata > 0:
            list_hour=self.df_sdp_today['HOUR'].drop_duplicates().tolist()
            rawatt=self.df_sdp_today[self.df_sdp_today['ACCESSFLAG']==int(accflag)]
            rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
            dfhourlyatt=rawatt[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
        else :
            dfhourlyatt=[]
            dfhourlysuc=[]
            list_hour=[]
        return dfhourlyatt['TOTAL'].tolist(),dfhourlysuc['TOTAL'].tolist(),list_hour
        

    

    

