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
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'], errors='ignore')
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].fillna(0)
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception:
            self.flagdata=0

    def SumDataToday(self):
        if self.flagdata > 0 :
            today=GetToday()
            self.df_scp_today=self.dataraw[self.dataraw['DATE'] == today.date()]
            self.dfscpsuc=self.df_scp_today[self.df_scp_today['DIAMETER'].isin(list_diameter)]
            scpatt=pd.Series(self.df_scp_today['TOTAL']).sum()
            scpsuc=pd.Series(self.dfscpsuc['TOTAL']).sum()
            scpsr=round((scpsuc/scpatt)*100,2)
        else :
            scpatt='N/A'
            scpsuc='N/A'
            scpsr='N/A'
        return scpatt,scpsuc,scpsr
    
    def VerifyData(self):
        return self.dataraw

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
    
    
    def HourMinScp(self):
        if self.flagdata > 0 :
            self.df_scp_today['hourmin']=self.df_scp_today['CDRDATE'].apply(lambda x: str(x)[11:] )
            dfrawminute=self.df_scp_today[['hourmin','TOTAL']].groupby('hourmin').sum().reset_index()
            dfminute=dfrawminute.iloc[-25:]
            list_scpatt=dfminute['TOTAL'].tolist()
            list_scpmin=dfminute['hourmin'].tolist()
        else :
            list_scpatt=[]
            list_scpmin=[]
        return list_scpatt,list_scpmin

        
    def Att3Days(self):
        if self.flagdata > 0 :
            dfhourlyatt=pd.pivot_table(self.dataraw,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh

    def AttSk3Days(self,servicekey=None):
        if self.flagdata > 0 :
            dffilter=self.dataraw[self.dataraw['SERVICE_KEY']==int(servicekey)]
            skatt=pd.pivot_table(dffilter,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return skatt['day0'].tolist(),skatt['day1'].tolist(),skatt['day7'].tolist(),skatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh
    
    def AttDia3Days(self,diameter=None):
        if self.flagdata > 0 :
            dffilter=self.dataraw[self.dataraw['DIAMETER']==int(diameter)]
            diaatt=pd.pivot_table(dffilter,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return diaatt['day0'].tolist(),diaatt['day1'].tolist(),diaatt['day7'].tolist(),diaatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh



class SdpData():

    def __init__(self,pathfile=None):
        try :
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'])
            self.dataraw['INTERNALCAUSE']=self.dataraw['INTERNALCAUSE'].fillna(0)
            self.dataraw['CPID']=self.dataraw['CPID'].fillna(0)
            self.dataraw['INTERNALCAUSE ']=self.dataraw['INTERNALCAUSE'].astype(int)
            self.dataraw['CPID']=self.dataraw['CPID'].astype(int)
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
    
    def VerifyData(self):
        return self.df_sdp_today

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
    
    def HourMinSdp(self):
        if self.flagdata > 0:
            self.df_sdp_today['hourmin']=self.df_sdp_today['CDRDATE'].apply(lambda x: str(x)[11:] )
            dfrawminute=pd.pivot_table(self.df_sdp_today,values='TOTAL', index=['hourmin'],columns=['ACCESSFLAG'], aggfunc="sum", fill_value=0).reset_index()
            dfminute=dfrawminute.iloc[-25:]
            list_moatt=dfminute[66].tolist()
            list_mtatt=dfminute[67].tolist()
            list_diatt=dfminute[68].tolist()
            list_soatt=dfminute[72].tolist()
            list_statt=dfminute[73].tolist()
            list_min=dfminute['hourmin'].tolist()
        else :
            list_moatt=[]
            list_mtatt=[]
            list_diatt=[]
            list_soatt=[]
            list_statt=[]
            list_min=[]
        return list_moatt,list_mtatt,list_diatt,list_soatt,list_statt,list_min

        

    

    

