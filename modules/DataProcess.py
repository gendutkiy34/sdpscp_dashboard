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
            dfminute=dfrawminute.iloc[-20:]
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
            self.dataraw['ISBFT']=self.dataraw['ISBFT'].astype(int)
            self.dataraw['SERVICE_KEY']=self.dataraw['SERVICE_KEY'].astype(int)
            self.dataraw['DATE']=self.dataraw['CDRDATE2'].dt.date
            self.dataraw['HOUR']=self.dataraw['CDRDATE2'].dt.hour
            self.flagdata=1
        except Exception :
            self.flagdata=0
        self.datatoday=self.dataraw[self.dataraw['REMARK']=='day0']
        self.dfsuctoday=self.datatoday[self.datatoday['DIAMETER'].isin(list_diameter)]
        

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
        
    def VerifyDataToday(self):
        return self.datatoday
        
    def SumDataToday(self):
        scpatt='N/A'
        scpsuc='N/A'
        scpsr='N/A'
        list_dia=[]
        listsum=[]
        item={}
        if self.flagdata > 0 :
            self.dfscpsuc=self.datatoday[self.datatoday['DIAMETER'].isin(list_diameter)].reset_index()
            #sumarry today
            scpatt=pd.Series(self.datatoday['TOTAL']).sum()
            scpsuc=pd.Series(self.dfscpsuc['TOTAL']).sum()
            scpsr=round((scpsuc/scpatt)*100,2)
            item['flag']='ALL VOICE'
            item['attempt']=f'{scpatt:,}'
            item['success']=f'{scpsuc:,}'
            item['sr']=scpsr
            listsum.append(item)
            #diameter summary non 2001
            dfnonsuc=self.datatoday[~self.datatoday['DIAMETER'].isin([2001,0])]
            sumdiameter=dfnonsuc[['DIAMETER','TOTAL']].groupby('DIAMETER').sum().reset_index()
            sumdiameter=sumdiameter.sort_values('TOTAL',ascending=False)
            for d in sumdiameter.iterrows() :
                item={}
                item['errcode']=d[1][0]
                item['total']=f'{d[1][1]:,}'
                list_dia.append(item)
            #roaming summary
            roamsuc=self.dfscpsuc[['IS_ROAMING','TOTAL']].groupby('IS_ROAMING').sum().reset_index()
            roamsuc=roamsuc.rename(columns={'TOTAL':'SUCCESS'})
            roamatt=self.datatoday[['IS_ROAMING','TOTAL']].groupby('IS_ROAMING').sum().reset_index()
            roamatt=roamatt.rename(columns={'TOTAL':'ATTEMPT'})
            dfroamjoin=pd.merge(roamatt,roamsuc,on=['IS_ROAMING'],how='left').reset_index()
            dfroamjoin=dfroamjoin.assign(SUCCES_RATE=lambda x : round((x['SUCCESS']/x['ATTEMPT'])*100,2))
            print(dfroamjoin)
            for d in dfroamjoin[['IS_ROAMING','ATTEMPT','SUCCESS','SUCCES_RATE']].iterrows() :
                item={}
                item['flag']= 'NON ROAMING' if d[1][0] == 0 else  'ROAMING'
                item['attempt']=f'{d[1][1]:,}'
                item['success']=f'{d[1][2]:,}'
                item['sr']=d[1][3]
                listsum.append(item)
            #sksummary
            skyatt=self.datatoday[['SERVICE_KEY','TOTAL']].groupby('SERVICE_KEY').sum().reset_index()
            skyatt=skyatt.rename(columns={'TOTAL':'ATTEMPT'})
            skysuc=self.dfscpsuc[['SERVICE_KEY','TOTAL']].groupby('SERVICE_KEY').sum().reset_index()
            skysuc=skysuc.rename(columns={'TOTAL':'SUCCESS'})
            skyjoin=pd.merge(skyatt,skysuc,on=['SERVICE_KEY'],how='left').reset_index()
            skyjoin=skyjoin.fillna(0)
            skyjoin['SUCCESS']=skyjoin['SUCCESS'].astype(int)
            skyjoin['ATTEMPT']=skyjoin['ATTEMPT'].astype(int)
            skyjoin=skyjoin.assign(SUCCES_RATE=lambda x : round((x['SUCCESS']/x['ATTEMPT'])*100,2))
            skyjoin['SERVICE_KEY']=skyjoin['SERVICE_KEY'].astype(int)
            for d in skyjoin[['SERVICE_KEY','ATTEMPT','SUCCESS','SUCCES_RATE']].iterrows() :
                item={}
                item['flag']= f'sk - {d[1][0]}'
                item['attempt']=f'{d[1][1]:,}'
                item['success']=f'{d[1][2]:,}'
                item['sr']=d[1][3]
                listsum.append(item)
        return scpatt,scpsuc,scpsr,list_dia,listsum
    
    def HourlyDataToday(self,roaming=None):
        listatthour=[]
        listsuchour=[]
        list_hour=[]
        listsrhour=[]
        if self.flagdata > 0 :
            list_hour=self.datatoday['HOUR'].drop_duplicates().tolist()
            if roaming is None :
                dfhourlyatt=self.datatoday[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=self.dfsuctoday[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                listatthour=dfhourlyatt['TOTAL'].tolist()
                listsuchour=dfhourlysuc['TOTAL'].tolist()     
                for a,s in zip(listatthour,listsuchour):
                    sr=round((s/a)*100,2) 
                    listsrhour.append(sr)
            else :
                dfroam=self.datatoday[self.datatoday['IS_ROAMING']==int(roaming)]
                dfroamsuc=dfroam[dfroam['DIAMETER'].isin(list_diameter)]
                dfhourlyatt=dfroam[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=dfroamsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                listatthour=dfhourlyatt['TOTAL'].tolist()
                listsuchour=dfhourlysuc['TOTAL'].tolist()     
                for a,s in zip(listatthour,listsuchour):
                    sr=round((s/a)*100,2) 
                    listsrhour.append(sr)
        return listatthour,listsuchour,listsrhour,list_hour
    
    def BftToday(self):
        listhour=[]
        listbft=[]
        listerr=[]
        listtotal=[]
        if self.flagdata > 0 :
            list_hour=self.datatoday['HOUR'].drop_duplicates().tolist()
            rawbft=self.datatoday[self.datatoday['ISBFT']==1]
            dfhour=self.datatoday[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            dfbft=rawbft[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            dfjoin=pd.merge(dfhour[['HOUR']],dfbft[['HOUR','TOTAL']],on='HOUR',how='left')
            dfjoin=dfjoin.fillna(0)
            dfjoin['TOTAL']=dfjoin['TOTAL'].astype(int)
            errbft=rawbft[['DIAMETER','TOTAL']].groupby('DIAMETER').sum().reset_index()
            listhour=dfjoin['HOUR'].to_list()
            listbft=dfjoin['TOTAL'].to_list()
            listerr=errbft['DIAMETER'].to_list()
            listtotal=errbft['TOTAL'].to_list()
        return listhour,listbft,listerr,listtotal
    
    def AttSkToday(self,servicekey=None,diameter=None):
        lisskatt=[]
        sksumatt=''
        if self.flagdata > 0 :
            if diameter is  None :
                dffilter1=self.datatoday
            else :
                dffilter1=self.dataraw[self.dataraw['DIAMETER']==int(diameter)]
            if servicekey is None :
                dffilter2=dffilter1
            else :
                dffilter2=dffilter1[dffilter1['SERVICE_KEY']==int(servicekey)]
            skatthour=dffilter2[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            sksumatt=skatthour['TOTAL'].sum()
            lisskatt=skatthour['TOTAL'].to_list()
        return lisskatt,sksumatt
    
    def AttRoamToday(self,servicekey=None,diameter=None,roaming=None):
        listroam=[]
        sumroam=''
        if self.flagdata > 0 :
            if servicekey is not None :
                dffilter1=self.datatoday[self.datatoday['SERVICE_KEY']==int(servicekey)]
            else :
                dffilter1=self.datatoday
            if diameter is not None :
                dffilter2=dffilter1[dffilter1['DIAMETER']==int(diameter)]
            else :
                dffilter2=dffilter1
            if roaming is not None :
                dffilter=dffilter2[dffilter2['IS_ROAMING']==int(roaming)]
            else :
                dffilter=dffilter2
        dfroam=dffilter[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
        summ=dffilter['TOTAL'].sum()
        listroam=dfroam['TOTAL'].to_list()
        return listroam,summ


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
            self.today=GetToday()
            self.df_sdp_today=self.dataraw[self.dataraw['DATE']== self.today.date()]
        except Exception :
            self.flagdata=0
    
    def SumDataToday(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                rawatt=self.df_sdp_today
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
            else :
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
                dfhourlyatt=self.df_sdp_today[['HOUR','TOTAL']].groupby('HOUR').sum('TOTAL').reset_index()
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
            dfminute=dfrawminute.iloc[-20:]
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
        self.datatoday=self.dataraw[self.dataraw['REMARK']=='day0']
        self.dfsuc=self.dataraw[self.dataraw['INTERNALCAUSE'].isin(list_diameter) ]
    
    def VerifyDataRaw(self):
        return self.dataraw 
    
    def Revenue(self,accflag=None):
        listrev0=[]
        listrev1=[]
        listrev2=[]
        listrevhour=[]
        if self.flagdata > 0 :            
            if accflag is None :
                dfsuc=self.dfsuc[(self.dfsuc['BASICCAUSE'].isin(list_rev)) & (self.dfsuc['INTERNALCAUSE']==2001) ]
            else :
                dfsuc=self.dfsuc[(self.dfsuc['BASICCAUSE'].isin(list_rev))& (self.dfsuc['INTERNALCAUSE']==2001)&(self.dfsuc['ACCESSFLAG']==int(accflag))]
            dfhourlyatt=pd.pivot_table(dfsuc,values='REVENUE', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            listrev0=dfhourlyatt['day0'].tolist()
            listrev1=dfhourlyatt['day1'].tolist()
            listrev7=dfhourlyatt['day7'].tolist()
            listrevhour=dfhourlyatt['HOUR'].tolist()
            return listrev0,listrev1,listrev7,listrevhour

        
    def Att(self,accflag=None):
        if self.flagdata > 0 :
            if accflag is None :
                dfsuc=self.dfsuc[['HOUR','ACCESSFLAG','TOTAL']]
            else :
                dfsuc=self.dfsuc[self.dfsuc['ACCESSFLAG']==int(accflag)]
            dfhourlyatt=pd.pivot_table(dfsuc,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            return dfhourlyatt['day0'].tolist(),dfhourlyatt['day1'].tolist(),dfhourlyatt['day7'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt0=[]
            dfhourlyatt1=[]
            dfhourlyatt7=[]
            list_hour=[]
            return dfhourlyatt0,dfhourlyatt1,dfhourlyatt7,list_hour
        
    def Succ(self,accflag=None):
        dfhourlyatt0=[]
        dfhourlyatt1=[]
        dfhourlyatt7=[]
        list_hour=[]
        if self.flagdata > 0 :
            list_rev=[941,949,938]
            if accflag is None :
                dfsuc=self.dfsuc[['HOUR','ACCESSFLAG','TOTAL']]
            else :
                dfsuc=self.dfsuc[(self.dfsuc['ACCESSFLAG']==int(accflag))]
            dfhourlyatt=pd.pivot_table(dfsuc,values='TOTAL', index=['HOUR'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            dfhourlyatt0=dfhourlyatt['day0'].tolist()
            dfhourlyatt1=dfhourlyatt['day1'].tolist()
            dfhourlyatt7=dfhourlyatt['day7'].tolist()
            list_hour=dfhourlyatt['HOUR'].tolist()
        return dfhourlyatt0,dfhourlyatt1,dfhourlyatt7,list_hour
        
    def Summary(self):
        if self.flagdata > 0 :
            dfatt=pd.pivot_table(self.dataraw,values='TOTAL', index=['ACCESSFLAG'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            dfatt=dfatt.rename(columns={'day0':'att0','day1':'att1','day7':'att7'})
            dfsuc=pd.pivot_table(self.dfsuc,values='TOTAL', index=['ACCESSFLAG'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            dfsuc=dfsuc.rename(columns={'day0':'suc0','day1':'suc1','day7':'suc7'})
            rawrev=self.dfsuc[(self.dfsuc['BASICCAUSE'].isin(list_rev)) & ( self.dfsuc['INTERNALCAUSE']==2001)]
            dfrev=pd.pivot_table(self.dfsuc,values='REVENUE', index=['ACCESSFLAG'],columns=['REMARK'], aggfunc="sum", fill_value=0).reset_index()
            dfjoin=pd.merge(dfatt,dfsuc,on=['ACCESSFLAG'])
            listd=[0,1,7]
            dfjoin=dfjoin.assign(sr0=lambda x :round((x.suc0/x.att0)*100,2))
            dfjoin=dfjoin.assign(sr1=lambda x :round((x.suc1/x.att1)*100,2))
            dfjoin=dfjoin.assign(sr7=lambda x :round((x.suc7/x.att7)*100,2))
            dfsr=dfjoin[['ACCESSFLAG','sr0','sr1','sr7']]
            return dfatt,dfsuc,dfsr,dfrev
        
    def DataToday(self):
        return self.datatoday
    
    def SrHourToday(self,accflag=None) :
        if self.flagdata > 0:
            if accflag is None :
                dfhourlyatt=self.datatoday[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlyatt=dfhourlyatt.rename(columns={'TOTAL':'ATTEMPT'})
                rawsuc=self.datatoday[self.datatoday['INTERNALCAUSE'].isin(list_diameter)]
                dfhourlysuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                dfhourlysuc=dfhourlysuc.rename(columns={'TOTAL':'SUCCESS'})
                dfjoin=pd.merge(dfhourlyatt,dfhourlysuc,on=['HOUR'])
                dfjoin['SUCCESS_RATE']=dfjoin.apply(lambda x : round(x['SUCCESS']/x['ATTEMPT']*100,2) ,axis=1)
            else :
                rawatt=self.datatoday[self.datatoday['ACCESSFLAG']==int(accflag)]
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
    
    def AttHourToday(self,accflag=None):
        if self.flagdata > 0:
            self.list_hour=self.datatoday['HOUR'].drop_duplicates().tolist()
            if accflag is None :
                houratt=self.datatoday[['HOUR','TOTAL']].groupby('HOUR').sum('TOTAL').reset_index()
                rawsuc=self.datatoday[self.datatoday['INTERNALCAUSE'].isin(list_diameter)]
                hoursuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            else :
                rawatt=self.datatoday[self.datatoday['ACCESSFLAG']==int(accflag)]
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
                houratt=rawatt[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
                hoursuc=rawsuc[['HOUR','TOTAL']].groupby('HOUR').sum().reset_index()
            return houratt['TOTAL'].tolist(),hoursuc['TOTAL'].tolist(),self.list_hour
        else :
            houratt=[]
            hoursuc=[]      
            list_hour=[]
            return  houratt,hoursuc,list_hour   

    def RevToday(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                dfsuc=self.datatoday[(self.datatoday['INTERNALCAUSE']==2001) & (self.datatoday['BASICCAUSE'].isin(list_rev))]
            else :
                dfsuc=self.datatoday[(self.datatoday['INTERNALCAUSE']==2001)& (self.datatoday['BASICCAUSE'].isin(list_rev)) & (self.datatoday['ACCESSFLAG']==int(accflag)) ]
            dfhourlyatt=dfsuc.groupby('HOUR')['REVENUE'].sum('REVENUE').reset_index()   
            return dfhourlyatt['REVENUE'].tolist(),dfhourlyatt['HOUR'].tolist()
        else :
            dfhourlyatt=[]
            list_hour=[]
            return dfhourlyatt 
    
    def SumAccToday(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                rawatt=self.datatoday
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
            else :
                rawatt=self.datatoday[self.datatoday['ACCESSFLAG']==int(accflag)]
                rawsuc=rawatt[rawatt['INTERNALCAUSE'].isin(list_diameter)]
            sdpatt=pd.Series(rawatt['TOTAL']).sum()
            sdpsuc=pd.Series(rawsuc['TOTAL']).sum()
            sdpsr=round((sdpsuc/sdpatt)*100,2)
        else :
            sdpatt='N/A'
            sdpsuc='N/A'
            sdpsr='N/A'
        return sdpatt,sdpsuc,sdpsr
    
    def RevTop5(self,accflag=None):
        if self.flagdata > 0:
            if accflag is None :
                dfsuc=self.datatoday[(self.datatoday['INTERNALCAUSE']==2001) & (self.datatoday['BASICCAUSE'].isin(list_rev))]
            else :
                dfsuc=self.datatoday[(self.datatoday['INTERNALCAUSE']==2001)& (self.datatoday['BASICCAUSE'].isin(list_rev)) & (self.datatoday['ACCESSFLAG']==int(accflag)) ]
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
                dfsuc=self.datatoday
            else :
                dfsuc=self.datatoday[ (self.datatoday['ACCESSFLAG']==int(accflag)) ]
            dfhourlyatt=dfsuc.groupby('CP_NAME')['TOTAL'].sum('TOTAL').reset_index()
            dftop=dfhourlyatt.sort_values('TOTAL',ascending=False).head(5)
            listotal=[ f'{d:}' for d in dftop['TOTAL'].tolist()]
            return dftop['CP_NAME'].tolist(),listotal
        else :
            listcp=[]
            listrev=[]
            return listcp,listrev
    
    def SummaryToday(self):
        if self.flagdata > 0:
            dfatt=self.datatoday.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
            dfatt=dfatt.rename(columns={'TOTAL':'ATTEMPT'})
            rawsuc=self.datatoday[self.datatoday['INTERNALCAUSE'].isin(list_diameter)]
            dfsuc=rawsuc.groupby('ACCESSFLAG')['TOTAL'].sum('TOTAL').reset_index()
            dfsuc=dfsuc.rename(columns={'TOTAL':'SUCCESS'})
            rawrev=self.datatoday[(self.datatoday['INTERNALCAUSE']==2001) & (self.datatoday['BASICCAUSE'].isin(list_rev))]
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

     


    
