import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.remoteclient import GetRemoteFile


#variable
path=os.getcwd()
remote_dir='/home/sdpuser/TechM_Sdp/output/'
inputscp='{0}/rawdata/scp_raw_hour.csv'.format(path)
inputsdp='{0}/rawdata/sdp_raw_hour.csv'.format(path)

scp_colum=['CDR_HOUR', 'ATT_0', 'ATT_1', 'ATT_7', 'SUC_0', 'SUC_1',
       'SUC_7', 'ROAMATT_0', 'ROAMATT_1', 'ROAMATT_7', 'ROAMSUC_0', 'ROAMSUC',
       'ROAMSUC_7', 'NONROATT_0', 'NONROATT_1', 'NONROATT_7', 'NONROSUC_0',
       'NONROSUC_1', 'NONROSUC_7', 'MOCATT_0', 'MOCATT_1', 'MOCATT_7',
       'MOCSUC_0', 'MOCSUC_1', 'MOCSUC_7', 'MTCATT_0', 'MTCATT_1', 'MTCATT_7',
       'MTCSUC_0', 'MTCSUC_1', 'MTCSUC_7', 'MFCATT_0', 'MFCATT_1', 'MFCATT_7',
       'MFCSUC_0', 'MFCSUC_1', 'MFCSUC_7','100ATT_0','100ATT_1','100ATT_7',
       '100SUC_0','100SUC_1','100SUC_7','150ATT_0','150ATT_1','150ATT_7',
       '150SUC_0','150SUC_1','150SUC_7','200ATT_0','200ATT_1','200ATT_7',
       '200SUC_0','200SUC_1','200SUC_7','300ATT_0','300ATT_1','300ATT_7',
       '300SUC_0','300SUC_1','300SUC_7','MMATT_0','MMATT_1','MMATT_7',
       'MMSUC_0','MMSUC_1','MMSUC_7', 'PKATT_0','PKATT_1','PKATT_7',
       'PKSUC_0','PKSUC_1','PKSUC_7']

sdp_colum=['CDR_HOUR', 'ATT_0', 'ATT_1', 'ATT_7', 'SUC_0', 'SUC_1', 'SUC_7',
       'BMOATT_0', 'BMOATT_1', 'BMOATT_7', 'BMOSUC_0', 'BMOSUC_1', 'BMOSUC_7',
       'BMTATT_0', 'BMTATT_1', 'BMTATT_7', 'BMTSUC_0', 'BMTSUC_1', 'BMTSUC_7',
       'DIGATT_0', 'DIGATT_1', 'DIGATT_7', 'DIGSUC_0', 'DIGSUC_1',
       'DIGSUC_7', 'SMOATT_0', 'SMOATT_1', 'SMOATT_7', 'SMOSUC_0',
       'SMOSUC_1', 'SMOSUC_7', 'SMTATT_0', 'SMTATT_1', 'SMTATT_7', 'SMTSUC_0',
       'SMTSUC_1', 'SMTSUC_7']
#'RNWATT_0', 'RNWATT_1', 'RNWATT_7', 'RNWSUC_0','RNWSUC_1', 'RNWSUC_7'

def SftpFile():
    pathdir=os.getcwd()
    creden=f'{pathdir}/connections/mgwsit.json'
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}scp_raw_hour.csv',localdir=inputscp)
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}sdp_raw_hour.csv',localdir=inputsdp)


def ScpProcessHour():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/scp_newdata_hour.csv'
    try :
        dfraw=pd.read_csv(inputscp)
        flag=1
    except Exception :
        pass
    if flag == 1 :
        #new data raw
        dfraw.fillna(0, inplace=True)
        dfraw['CALL_TYPE']=dfraw['CALL_TYPE'].astype('int')
        dfraw['IS_ROAMING']=dfraw['IS_ROAMING'].astype('int')
        dfraw['SERVICE_KEY']=dfraw['SERVICE_KEY'].astype('int')
        dfraw['TOTAL']=dfraw['TOTAL'].astype('int')

        #filter data raw
        dfrawmoc=dfraw[dfraw['CALL_TYPE']== 1 ]
        dfrawmtc=dfraw[dfraw['CALL_TYPE']== 2 ]
        dfrawmfc=dfraw[dfraw['CALL_TYPE']== 3 ]
        dfrawroam=dfraw[dfraw['IS_ROAMING']== 1 ]
        dfrawnonroam=dfraw[dfraw['IS_ROAMING']== 0 ]
        dfraw100=dfraw[dfraw['SERVICE_KEY']== 100 ]
        dfraw150=dfraw[dfraw['SERVICE_KEY']== 150 ]
        dfraw200=dfraw[dfraw['SERVICE_KEY']== 200 ]
        dfraw300=dfraw[dfraw['SERVICE_KEY']== 300 ]
        dfrawmm=dfraw[dfraw['INSTANCE_ID'] == 'MM' ]
        dfrawpk=dfraw[dfraw['INSTANCE_ID'] == 'PK' ]
        rawsuc=dfraw[dfraw['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawmocsuc=dfrawmoc[dfrawmoc['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawmtcsuc=dfrawmtc[dfrawmtc['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawmfcsuc=dfrawmfc[dfrawmfc['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawroamsuc=dfrawroam[dfrawroam['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawnonroamsuc=dfrawnonroam[dfrawnonroam['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        raw100suc=dfraw100[dfraw100['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        raw150suc=dfraw150[dfraw150['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        raw200suc=dfraw200[dfraw200['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        raw300suc=dfraw300[dfraw300['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawmmsuc=dfrawmm[dfrawmm['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]
        rawpksuc=dfrawpk[dfrawpk['DIAMETER_RESULT_CODES'].isin([2001,4010,4012,5030,5031])]

        #pivot table
        dfatt=pd.pivot_table(dfraw,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfatt=dfatt.rename(columns={'day0':'ATT_0','day1':'ATT_1','day7':'ATT_7'})
        dfsuc=pd.pivot_table(rawsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsuc=dfsuc.rename(columns={'day0':'SUC_0','day1':'SUC_1','day7':'SUC_7'})
        dfmocatt=pd.pivot_table(dfrawmoc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmocatt=dfmocatt.rename(columns={'day0':'MOCATT_0','day1':'MOCATT_1','day7':'MOCATT_7'})
        dfmocsuc=pd.pivot_table(rawmocsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmocsuc=dfmocsuc.rename(columns={'day0':'MOCSUC_0','day1':'MOCSUC_1','day7':'MOCSUC_7'})
        dfmtcatt=pd.pivot_table(dfrawmtc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmtcatt=dfmtcatt.rename(columns={'day0':'MTCATT_0','day1':'MTCATT_1','day7':'MTCATT_7'})
        dfmtcsuc=pd.pivot_table(rawmtcsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmtcsuc=dfmtcsuc.rename(columns={'day0':'MTCSUC_0','day1':'MTCSUC_1','day7':'MTCSUC_7'})
        dfmfcatt=pd.pivot_table(dfrawmfc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmfcatt=dfmfcatt.rename(columns={'day0':'MFCATT_0','day1':'MFCATT_1','day7':'MFCATT_7'})
        dfmfcsuc=pd.pivot_table(rawmfcsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmfcsuc=dfmfcsuc.rename(columns={'day0':'MFCSUC_0','day1':'MFCSUC_1','day7':'MFCSUC_7'})
        dfroamatt=pd.pivot_table(dfrawroam,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfroamatt=dfroamatt.rename(columns={'day0':'ROAMATT_0','day1':'ROAMATT_1','day7':'ROAMATT_7'})
        dfroamsuc=pd.pivot_table(rawroamsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfroamsuc=dfroamsuc.rename(columns={'day0':'ROAMSUC_0','day1':'ROAMSUC','day7':'ROAMSUC_7'})
        dfnonroamatt=pd.pivot_table(dfrawnonroam,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfnonroamatt=dfnonroamatt.rename(columns={'day0':'NONROATT_0','day1':'NONROATT_1','day7':'NONROATT_7'})
        dfnonroamsuc=pd.pivot_table(rawnonroamsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfnonroamsuc=dfnonroamsuc.rename(columns={'day0':'NONROSUC_0','day1':'NONROSUC_1','day7':'NONROSUC_7'})
        df100att=pd.pivot_table(dfraw100,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df100att=df100att.rename(columns={'day0':'100ATT_0','day1':'100ATT_1','day7':'100ATT_7'})
        df100suc=pd.pivot_table(raw100suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df100suc=df100suc.rename(columns={'day0':'100SUC_0','day1':'100SUC_1','day7':'100SUC_7'})
        df150att=pd.pivot_table(dfraw150,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df150att=df150att.rename(columns={'day0':'150ATT_0','day1':'150ATT_1','day7':'150ATT_7'})
        df150suc=pd.pivot_table(raw150suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df150suc=df150suc.rename(columns={'day0':'150SUC_0','day1':'150SUC_1','day7':'150SUC_7'})
        df200att=pd.pivot_table(dfraw200,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df200att=df200att.rename(columns={'day0':'200ATT_0','day1':'200ATT_1','day7':'200ATT_7'})
        df200suc=pd.pivot_table(raw200suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df200suc=df200suc.rename(columns={'day0':'200SUC_0','day1':'200SUC_1','day7':'200SUC_7'})
        df300att=pd.pivot_table(dfraw300,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df300att=df300att.rename(columns={'day0':'300ATT_0','day1':'300ATT_1','day7':'300ATT_7'})
        df300suc=pd.pivot_table(raw300suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        df300suc=df300suc.rename(columns={'day0':'300SUC_0','day1':'300SUC_1','day7':'300SUC_7'})
        dfmmatt=pd.pivot_table(dfrawmm,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmmatt=dfmmatt.rename(columns={'day0':'MMATT_0','day1':'MMATT_1','day7':'MMATT_7'})
        dfmmsuc=pd.pivot_table(rawmmsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfmmsuc=dfmmsuc.rename(columns={'day0':'MMSUC_0','day1':'MMSUC_1','day7':'MMSUC_7'})
        dfpkatt=pd.pivot_table(dfrawpk,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfpkatt=dfpkatt.rename(columns={'day0':'PKATT_0','day1':'PKATT_1','day7':'PKATT_7'})
        dfpksuc=pd.pivot_table(rawpksuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfpksuc=dfpksuc.rename(columns={'day0':'PKSUC_0','day1':'PKSUC_1','day7':'PKSUC_7'})
  
        #join table
        dfjoin1=pd.merge(pd.merge(dfatt,dfsuc,on=['CDR_HOUR']),dfroamatt,on=['CDR_HOUR'],how='left')
        dfjoin2=pd.merge(pd.merge(dfjoin1,dfroamsuc,on=['CDR_HOUR'],how='left'),dfnonroamatt,on=['CDR_HOUR'],how='left')
        dfjoin3=pd.merge(pd.merge(dfjoin2,dfnonroamsuc,on=['CDR_HOUR'],how='left'),dfmocatt,on=['CDR_HOUR'],how='left')
        dfjoin4=pd.merge(pd.merge(dfjoin3,dfmocsuc,on=['CDR_HOUR'],how='left'),dfmtcatt,on=['CDR_HOUR'],how='left')
        dfjoin5=pd.merge(pd.merge(dfjoin4,dfmtcsuc,on=['CDR_HOUR'],how='left'),dfmfcatt,on=['CDR_HOUR'],how='left')
        dfjoin6=pd.merge(pd.merge(dfjoin5,dfmfcsuc,on=['CDR_HOUR'],how='left'),df100att,on=['CDR_HOUR'],how='left')
        dfjoin7=pd.merge(pd.merge(dfjoin6,df100suc,on=['CDR_HOUR'],how='left'),df150att,on=['CDR_HOUR'],how='left')
        dfjoin8=pd.merge(pd.merge(dfjoin7,df150suc,on=['CDR_HOUR'],how='left'),df200att,on=['CDR_HOUR'],how='left')
        dfjoin9=pd.merge(pd.merge(dfjoin8,df200suc,on=['CDR_HOUR'],how='left'),df300att,on=['CDR_HOUR'],how='left')
        dfjoin10=pd.merge(pd.merge(dfjoin9,df300suc,on=['CDR_HOUR'],how='left'),dfmmatt,on=['CDR_HOUR'],how='left')
        dfjoin11=pd.merge(pd.merge(dfjoin10,dfmmsuc,on=['CDR_HOUR'],how='left'),dfpkatt,on=['CDR_HOUR'],how='left')
        joinfinal=pd.merge(dfjoin11,dfpksuc,on=['CDR_HOUR'],how='left').reset_index()
        join_column=joinfinal.columns
        print(joinfinal.info())
        for c in scp_colum:
            if c not in join_column:
                joinfinal[c]=0
        dffinal=joinfinal[scp_colum]
        dffinal.fillna(0, inplace=True)
        dffinal.to_csv(outputfile,index=False)


def SdpProcessHour():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/sdp_newdata_hour.csv'
    try :
        dfraw=pd.read_csv(inputsdp)
        flag=1
    except Exception :
        pass
    
    if flag == 1 :
        #new data raw
        dfraw.fillna(0, inplace=True)
        dfraw['ACCESSFLAG']=dfraw['ACCESSFLAG'].astype('int')
        #dfraw['INTERNALCAUSE']=dfraw['INTERNALCAUSE'].astype('int')
        dfraw['TOTAL']=dfraw['TOTAL'].astype('int')

        #filter data raw
        dfrawbmo=dfraw[dfraw['ACCESSFLAG']== 66 ]
        dfrawbmt=dfraw[dfraw['ACCESSFLAG']== 67 ]
        dfrawdig=dfraw[dfraw['ACCESSFLAG']== 68 ]
        dfrawsmo=dfraw[dfraw['ACCESSFLAG']== 72 ]
        dfrawsmt=dfraw[dfraw['ACCESSFLAG']== 73 ]
        rawsuc=dfraw[dfraw['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
        rawbmosuc=dfrawbmo[dfrawbmo['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
        rawbmtsuc=dfrawbmt[dfrawbmt['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
        rawdigsuc=dfrawdig[dfrawdig['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
        rawsmosuc=dfrawsmo[dfrawsmo['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
        rawsmtsuc=dfrawsmt[dfrawsmt['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
    
        #pivot table
        dfatt=pd.pivot_table(dfraw,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfatt=dfatt.rename(columns={'day0':'ATT_0','day1':'ATT_1','day7':'ATT_7'})
        dfsuc=pd.pivot_table(rawsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsuc=dfsuc.rename(columns={'day0':'SUC_0','day1':'SUC_1','day7':'SUC_7'})
        dfbmoatt=pd.pivot_table(dfrawbmo,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfbmoatt=dfbmoatt.rename(columns={'day0':'BMOATT_0','day1':'BMOATT_1','day7':'BMOATT_7'})
        dfbmosuc=pd.pivot_table(rawbmosuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfbmosuc=dfbmosuc.rename(columns={'day0':'BMOSUC_0','day1':'BMOSUC_1','day7':'BMOSUC_7'})
        dfbmtatt=pd.pivot_table(dfrawbmt,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfbmtatt=dfbmtatt.rename(columns={'day0':'BMTATT_0','day1':'BMTATT_1','day7':'BMTATT_7'})
        dfbmtsuc=pd.pivot_table(rawbmtsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfbmtsuc=dfbmtsuc.rename(columns={'day0':'BMTSUC_0','day1':'BMTSUC_1','day7':'BMTSUC_7'})
        dfdigatt=pd.pivot_table(dfrawdig,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfdigatt=dfdigatt.rename(columns={'day0':'DIGATT_0','day1':'DIGATT_1','day7':'DIGATT_7'})
        dfdigsuc=pd.pivot_table(rawdigsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfdigsuc=dfdigsuc.rename(columns={'day0':'DIGSUC_0','day1':'DIGSUC_1','day7':'DIGSUC_7'})
        dfsmoatt=pd.pivot_table(dfrawsmo,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsmoatt=dfsmoatt.rename(columns={'day0':'SMOATT_0','day1':'SMOATT_1','day7':'SMOATT_7'})
        dfsmosuc=pd.pivot_table(rawsmosuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsmosuc=dfsmosuc.rename(columns={'day0':'SMOSUC_0','day1':'SMOSUC_1','day7':'SMOSUC_7'})
        dfsmtatt=pd.pivot_table(dfrawsmt,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsmtatt=dfsmtatt.rename(columns={'day0':'SMTATT_0','day1':'SMTATT_1','day7':'SMTATT_7'})
        dfsmtsuc=pd.pivot_table(rawsmtsuc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
        dfsmtsuc=dfsmtsuc.rename(columns={'day0':'SMTSUC_0','day1':'SMTSUC_1','day7':'SMTSUC_7'})
    
        #jointable
        dfjoin1=pd.merge(pd.merge(dfatt,dfsuc,on=['CDR_HOUR'],how='left'),dfbmoatt,on=['CDR_HOUR'],how='left')
        dfjoin2=pd.merge(pd.merge(dfjoin1,dfbmosuc,on=['CDR_HOUR'],how='left'),dfbmtatt,on=['CDR_HOUR'],how='left')
        dfjoin3=pd.merge(pd.merge(dfjoin2,dfbmtsuc,on=['CDR_HOUR'],how='left'),dfdigatt,on=['CDR_HOUR'],how='left')
        dfjoin4=pd.merge(pd.merge(dfjoin3,dfdigsuc,on=['CDR_HOUR'],how='left'),dfsmoatt,on=['CDR_HOUR'],how='left')
        dfjoin5=pd.merge(pd.merge(dfjoin4,dfsmosuc,on=['CDR_HOUR'],how='left'),dfsmtatt,on=['CDR_HOUR'],how='left')
        joinfinal=pd.merge(dfjoin5,dfsmtsuc,on=['CDR_HOUR'],how='left').reset_index()
        join_column=joinfinal.columns
        for c in sdp_colum:
            if c not in join_column:
                joinfinal[c]=0
        dffinal=joinfinal[sdp_colum]
        dffinal.fillna(0, inplace=True)
        dffinal.to_csv(outputfile,index=False)



def ErrorMonitor():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/sdpscp_error_monitor.csv'
    try :
        rawsdp=pd.read_csv(inputsdp)
        rawscp=pd.read_csv(inputscp)
        flag=1
    except Exception :
        pass

    if flag == 1 :
        list_col=['CDR_DATE', 'CDR_HOUR', 'ERR_5000', 'ERR_6000', 'ERR_5005', 'ERR_5004', 'ERR_83',
       'ERR_601', 'ERR_940']
        
        #scp
        scp_today=rawscp[rawscp['REMARK']== 'day0']
        raw_bft=scp_today[ (rawscp['ISBFT']==1 ) & ( rawscp['REMARK']== 'day0')]
        raw_bft['DIAMETER_RESULT_CODES']=raw_bft['DIAMETER_RESULT_CODES'].astype(int)
        df_hour=scp_today[['CDR_DATE','CDR_HOUR']].drop_duplicates()
        bft_pivot=pd.pivot_table(raw_bft,values="TOTAL",index=['CDR_DATE','CDR_HOUR'],columns=["DIAMETER_RESULT_CODES"],aggfunc={'TOTAL': "sum"}).reset_index()
        bft_pivot.fillna(0,inplace=True)
        for c in bft_pivot.columns :
            if c == 'DIAMETER_RESULT_CODES' or c == 'CDR_DATE'  or c == 'CDR_HOUR' :
                pass
            else :
                bft_pivot[c]=bft_pivot[c].astype(int)
        
        #sdp
        sdp_today=rawsdp[rawsdp['REMARK']== 'day0']   
        noncharging_base=sdp_today[sdp_today['BASICCAUSE'].isin([601,83])]
        charging_base=sdp_today[(sdp_today['BASICCAUSE']==940) & (sdp_today['INTERNALCAUSE'].isna())]                       
        noncharging_pivot=pd.pivot_table(noncharging_base,values="TOTAL",index=['CDR_DATE','CDR_HOUR'],columns=["BASICCAUSE"],aggfunc={'TOTAL': "sum"}).reset_index()
        noncharging_pivot.fillna(0,inplace=True)
        for c in noncharging_pivot.columns :
            if c == 'BASICCAUSE' or c == 'CDR_DATE'  or c == 'CDR_HOUR' :
                pass
            else :
                noncharging_pivot[c]=noncharging_pivot[c].astype(int)
        charging_pivot=pd.pivot_table(charging_base,values="TOTAL",index=['CDR_DATE','CDR_HOUR'],columns=["BASICCAUSE"],aggfunc={'TOTAL': "sum"}).reset_index()
        charging_pivot.fillna(0,inplace=True)        
        
        #join
        join1=df_hour.merge(bft_pivot,how='left',on=['CDR_DATE','CDR_HOUR'])
        join2=join1.merge(noncharging_pivot,how='left',on=['CDR_DATE','CDR_HOUR'])
        finaljoin=join2.merge(charging_pivot,how='left',on=['CDR_DATE','CDR_HOUR'])
        finaljoin.fillna(0,inplace=True)
        for c in finaljoin.columns :
            if c == 'BASICCAUSE' or c == 'CDR_DATE'  or c == 'CDR_HOUR' :
                pass
            else :
                finaljoin[c]=finaljoin[c].astype(int)
                finaljoin.rename(columns={c:f'ERR_{c}'}, inplace = True)
        for c in list_col :
            if c not in finaljoin.columns :
                finaljoin[c] = 0 
        dffinal=finaljoin[list_col]
        dffinal.to_csv(outputfile,index=False)


SftpFile()
ScpProcessHour()
SdpProcessHour()
ErrorMonitor()

