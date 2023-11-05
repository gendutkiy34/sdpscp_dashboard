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
list_rev=[941,949,938]
services={66:'BULK MT',67:'BULK MO',68:'DIGITAL SERVICES',72:'SUBSCRIPTION MO',73:'SUBSCRIPTION MT'}



class ScpData():

    def __init__(self,pathfile=None):
        try : 
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'], format='%Y-%m-%d %H:%M')
            self.dataraw=self.dataraw.fillna(0)
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].astype(int)
            self.dataraw['TOTAL']=self.dataraw['TOTAL'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception:
            self.flagdata=0
        self.today=GetToday()
        self.df_scp_today=self.dataraw[self.dataraw['DATE'] == self.today.date()]
        self.dfscpsuc=self.df_scp_today[self.df_scp_today['DIAMETER'].isin(list_diameter)]

    def SumDataToday(self):
        if self.flagdata > 0 :
            scpatt=pd.Series(self.df_scp_today['TOTAL']).sum()
            scpsuc=pd.Series(self.dfscpsuc['TOTAL']).sum()
            scpsr=round((scpsuc/scpatt)*100,2)
        else :
            scpatt='N/A'
            scpsuc='N/A'
            scpsr='N/A'
        return scpatt,scpsuc,scpsr
    
    def VerifyDataRaw(self):
        return self.dataraw
    
    def VerifyDataToday(self):
        return self.df_scp_today

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



class ScpDataD017():

    def __init__(self,pathfile=None):
        try :
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'], format='%Y-%m-%d %H')
            self.dataraw=self.dataraw.fillna(0)
            self.dataraw['DIAMETER']=self.dataraw['DIAMETER'].astype(int)
            self.dataraw['TOTAL']=self.dataraw['TOTAL'].astype(int)
            self.dataraw['IS_ROAMING']=self.dataraw['IS_ROAMING'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception :
            self.flagdata=0

    def VerifyDataRaw(self):
        return self.dataraw 

    def Att(self):
        if self.flagdata > 0 :
            dfhourlyatt=pd.pivot_table(self.dataraw,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh

    def AttSk(self,servicekey=None,diameter=None):
        if self.flagdata > 0 :
            if diameter is  None :
                dffilter=self.dataraw[self.dataraw['SERVICE_KEY']==int(servicekey)]
            else :
                dffilter=self.dataraw[(self.dataraw['SERVICE_KEY']==int(servicekey)) & (self.dataraw['DIAMETER']==int(diameter))]
            skatt=pd.pivot_table(dffilter,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return skatt['day0'].tolist(),skatt['day1'].tolist(),skatt['day7'].tolist(),skatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh
        
    def AttRoam(self,servicekey=None,diameter=None,roaming=None):
        if self.flagdata > 0 :
            if servicekey is not None :
                dffilter1=self.dataraw[self.dataraw['SERVICE_KEY']==int(servicekey)]
            else :
                dffilter1=self.dataraw
            if diameter is not None :
                dffilter2=dffilter1[dffilter1['DIAMETER']==int(diameter)]
            else :
                dffilter2=dffilter1
            if roaming is not None :
                dffilter=dffilter2[dffilter2['IS_ROAMING']==int(roaming)]
            else :
                dffilter=dffilter2
            rmatt=pd.pivot_table(dffilter,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return rmatt['day0'].tolist(),rmatt['day1'].tolist(),rmatt['day7'].tolist(),rmatt['HOUR'].tolist()
        else :
            list0=[]
            list1=[]
            list7=[]
            listh=[]
            return list0,list1,list7,listh
    
    def AttDia(self,diameter=None):
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
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'], format='%Y-%m-%d %H:%M')
            self.dataraw=self.dataraw.fillna(0)
            self.dataraw['INTERNALCAUSE']=self.dataraw['INTERNALCAUSE'].astype(int)
            self.dataraw['BASICCAUSE']=self.dataraw['BASICCAUSE'].astype(int)
            self.dataraw['ACCESSFLAG']=self.dataraw['ACCESSFLAG'].astype(int)
            self.dataraw['TOTAL']=self.dataraw['TOTAL'].astype(int)
            self.dataraw['REVENUE']=self.dataraw['REVENUE'].astype(int)
            self.dataraw['CPID']=self.dataraw['CPID'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception :
            self.flagdata=0
        self.today=GetToday()
        self.df_sdp_today=self.dataraw[self.dataraw['DATE']== self.today.date()]
    
    def SumDataToday(self,accflag=None):
        if self.flagdata > 0:
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
    
    def VerifyDataRaw(self):
        return self.dataraw
    
    def VerifyDataToday(self):
        return self.df_sdp_today

    def AttHourToday(self,accflag=None):
        if self.flagdata > 0:
            list_hour=self.df_sdp_today['HOUR'].drop_duplicates().tolist()
            if accflag is None :
                dfhourlyatt=self.df_sdp_today[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                rawsuc=self.df_sdp_today[self.df_sdp_today['INTERNALCAUSE'].isin(list_diameter)]
                dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            else :
                rawatt=self.df_sdp_today[self.df_sdp_today['ACCESSFLAG']==int(accflag)]
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
                dfhourlyatt=rawatt[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            return dfhourlyatt['TOTAL'].tolist(),dfhourlysuc['TOTAL'].tolist(),list_hour
        else :
            dfhourlyatt=[]
            dfhourlysuc=[]
            list_hour=[]
            return dfhourlyatt,dfhourlysuc,list_hour
        
    def SucRatHourly(self,accflag=None) :
        if self.flagdata > 0:
            if accflag is None :
                dfhourlyatt=self.df_sdp_today[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlyatt=dfhourlyatt.rename(columns={'TOTAL':'ATTEMPT'})
                rawsuc=self.df_sdp_today[self.df_sdp_today['INTERNALCAUSE'].isin(list_diameter)]
                dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=dfhourlysuc.rename(columns={'TOTAL':'SUCCESS'})
                dfjoin=pd.merge(dfhourlyatt,dfhourlysuc,on=['HOUR'])
                dfjoin['SUCCESS_RATE']=dfjoin.apply(lambda x : round(x['SUCCESS']/x['ATTEMPT']*100,2) ,axis=1)
            else :
                rawatt=self.df_sdp_today[self.df_sdp_today['ACCESSFLAG']==int(accflag)]
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
                dfhourlyatt=rawatt[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlyatt=dfhourlyatt.rename(columns={'TOTAL':'ATTEMPT'})
                dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=dfhourlysuc.rename(columns={'TOTAL':'SUCCESS'})
                dfjoin=pd.merge(dfhourlyatt,dfhourlysuc,on=['HOUR'])
                dfjoin['SUCCESS_RATE']=dfjoin.apply(lambda x : round(x['SUCCESS']/x['ATTEMPT']*100,2) ,axis=1)
            return dfjoin['SUCCESS_RATE'].tolist(),dfjoin['HOUR'].tolist()
        else :
            dfhourlyatt=[]
            dfhourlysuc=[]
            list_hour=[]
            return dfhourlyatt,dfhourlysuc,list_hour
        
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
    
    def Revenue(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                dfsuc=self.df_sdp_today[(self.df_sdp_today['INTERNALCAUSE']==2001) & (self.df_sdp_today['BASICCAUSE'].isin(list_rev))]
            else :
                dfsuc=self.df_sdp_today[(self.df_sdp_today['INTERNALCAUSE']==2001)& (self.df_sdp_today['BASICCAUSE'].isin(list_rev)) & (self.df_sdp_today['ACCESSFLAG']==int(accflag)) ]
            dfhourlyatt=dfsuc.groupby('HOUR')['REVENUE'].sum('REVENUE').reset_index()   
            return dfhourlyatt['REVENUE'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt=[]
            list_hour=[]
            return dfhourlyatt,list_hour
        
    def RevTop5(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                dfsuc=self.df_sdp_today[(self.df_sdp_today['INTERNALCAUSE']==2001) & (self.df_sdp_today['BASICCAUSE'].isin(list_rev))]
            else :
                dfsuc=self.df_sdp_today[(self.df_sdp_today['INTERNALCAUSE']==2001)& (self.df_sdp_today['BASICCAUSE'].isin(list_rev)) & (self.df_sdp_today['ACCESSFLAG']==int(accflag)) ]
            dfhourlyatt=dfsuc.groupby('CP_NAME')['REVENUE'].sum('REVENUE').reset_index()
            dftop=dfhourlyatt.sort_values('REVENUE',ascending=False).head(5)
            lisrevenue=[ f'{d:,}' for d in dftop['REVENUE'].tolist()]
            return dftop['CP_NAME'].tolist(),lisrevenue
        else :
            listcp=[]
            listrev=[]
            return listcp,listrev
        
    def AttTop5(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                dfsuc=self.df_sdp_today
            else :
                dfsuc=self.df_sdp_today[ (self.df_sdp_today['ACCESSFLAG']==int(accflag)) ]
            dfhourlyatt=dfsuc.groupby('CP_NAME')['TOTAL'].sum('TOTAL').reset_index()
            dftop=dfhourlyatt.sort_values('TOTAL',ascending=False).head(5)
            listotal=[ f'{d:}' for d in dftop['TOTAL'].tolist()]
            return dftop['CP_NAME'].tolist(),listotal
        else :
            listcp=[]
            listrev=[]
            return listcp,listrev
        
    def Summary(self):
        if self.flagdata > 0:
            dfatt=self.df_sdp_today.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
            dfatt=dfatt.rename(columns={'TOTAL':'ATTEMPT'})
            rawsuc=self.df_sdp_today[self.df_sdp_today['INTERNALCAUSE'].isin(list_diameter)]
            dfsuc=rawsuc.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
            dfsuc=dfsuc.rename(columns={'TOTAL':'SUCCESS'})
            rawrev=self.df_sdp_today[(self.df_sdp_today['INTERNALCAUSE']==2001) & (self.df_sdp_today['BASICCAUSE'].isin(list_rev))]
            dfrev=rawrev.groupby('ACCESSFLAG')['REVENUE'].sum('REVENUE').reset_index()
            dfjoin1=pd.merge(dfatt[['ACCESSFLAG','ATTEMPT']],dfsuc[['ACCESSFLAG','SUCCESS']],on=['ACCESSFLAG'], how='left').reset_index()
            dffinaljoin=pd.merge(dfjoin1[['ACCESSFLAG','ATTEMPT','SUCCESS']],dfrev[['ACCESSFLAG','REVENUE']],on=['ACCESSFLAG'], how='left').reset_index()
            dffinaljoin=dffinaljoin.fillna(0)
            dffinaljoin['ATTEMPT']=dffinaljoin['ATTEMPT'].astype(int)
            dffinaljoin['SUCCESS']=dffinaljoin['SUCCESS'].astype(int)
            dffinaljoin['REVENUE']=dffinaljoin['REVENUE'].astype(int)
            dffinaljoin['ATTEMPT']=dffinaljoin.ATTEMPT.apply(lambda x : "{:,}".format(x))
            dffinaljoin['SUCCESS']=dffinaljoin.SUCCESS.apply(lambda x : "{:,}".format(x))
            dffinaljoin['REVENUE']=dffinaljoin.REVENUE.apply(lambda x : "{:,}".format(x))
            dffinaljoin['ACCESSFLAG']=dffinaljoin.ACCESSFLAG.apply(lambda x : services.get(x))
            listsum=dffinaljoin[['ACCESSFLAG','ATTEMPT','SUCCESS','REVENUE']].values.tolist()
            return listsum
        else :
            listsum=[]
            return listsum
        

        
class SdpDataD017():

    def __init__(self,pathfile=None):
        try :
            self.dataraw=pd.read_csv(pathfile)
            self.dataraw['CDRDATE2']=pd.to_datetime(self.dataraw['CDRDATE'], format='%Y-%m-%d %H')
            self.dataraw=self.dataraw.fillna(0)
            self.dataraw['INTERNALCAUSE']=self.dataraw['INTERNALCAUSE'].astype(int)
            self.dataraw['BASICCAUSE']=self.dataraw['BASICCAUSE'].astype(int)
            self.dataraw['ACCESSFLAG']=self.dataraw['ACCESSFLAG'].astype(int)
            self.dataraw['TOTAL']=self.dataraw['TOTAL'].astype(int)
            self.dataraw['REVENUE']=self.dataraw['REVENUE'].astype(int)
            self.dataraw['CPID']=self.dataraw['CPID'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception :
            self.flagdata=0
    
    def VerifyDataRaw(self):
        return self.dataraw 
    
    def Revenue(self,accessflag=None):
        if self.flagdata > 0 :
            list_rev=[941,949,938]
            if accessflag is None :
                dfsuc=self.dataraw[(self.dataraw['INTERNALCAUSE']==2001) & (self.dataraw['BASICCAUSE'].isin(list_rev)) ]
            else :
                dfsuc=self.dataraw[(self.dataraw['INTERNALCAUSE']==2001) & (self.dataraw['BASICCAUSE'].isin(list_rev)) & (self.dataraw['ACCESSFLAG']==int(accessflag))]
            dfhourlyatt=pd.pivot_table(dfsuc,values='REVENUE', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt0=[]
            dfhourlyatt1=[]
            dfhourlyatt7=[]
            list_hour=[]
            return dfhourlyatt0,dfhourlyatt1,dfhourlyatt7,list_hour
        
    def Att(self,accessflag=None):
        if self.flagdata > 0 :
            if accessflag is None :
                dfsuc=self.dataraw[['HOUR','ACCESSFLAG','TOTAL']]
            else :
                dfsuc=self.dataraw[self.dataraw['ACCESSFLAG']==int(accessflag)]
            dfhourlyatt=pd.pivot_table(dfsuc,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt0=[]
            dfhourlyatt1=[]
            dfhourlyatt7=[]
            list_hour=[]
            return dfhourlyatt0,dfhourlyatt1,dfhourlyatt7,list_hour
        
    def Succ(self,accessflag=None):
        if self.flagdata > 0 :
            list_rev=[941,949,938]
            if accessflag is None :
                dfsuc=self.dataraw[(self.dataraw['INTERNALCAUSE']==2001) & (self.dataraw['BASICCAUSE'].isin(list_rev)) ]
            else :
                dfsuc=self.dataraw[(self.dataraw['INTERNALCAUSE']==2001) & (self.dataraw['BASICCAUSE'].isin(list_rev)) & (self.dataraw['ACCESSFLAG']==int(accessflag))]
            dfhourlyatt=pd.pivot_table(dfsuc,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt0=[]
            dfhourlyatt1=[]
            dfhourlyatt7=[]
            list_hour=[]
            return dfhourlyatt0,dfhourlyatt1,dfhourlyatt7,list_hour
        

        
    
    
    
