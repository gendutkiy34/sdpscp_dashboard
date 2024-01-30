import pandas as pd
import time
import os
import schedule
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.remoteclient import GetRemoteFile
from GrepDataNew import GetDataMinute


#variable
path=os.getcwd()
remote_dir='/home/sdpuser/TechM_Sdp/output/'
inputscp='{0}/rawdata/scp_raw_minute.csv'.format(path)
inputsdp='{0}/rawdata/sdp_raw_minute.csv'.format(path)

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
list_diameter=[2001,4010,4012,5030,5031]

def SftpFile():
    pathdir=os.getcwd()
    creden=f'{pathdir}/connections/mgwsit.json'
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}scp_raw_minute.csv',localdir=inputscp)
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}sdp_raw_minute.csv',localdir=inputsdp)


def ScpProcessMinute():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/scp_newdata_minute.csv'
    try :
        dfraw=pd.read_csv(inputscp)
        flag=1
    except Exception :
        pass
    if flag == 1 :
        dfraw.fillna(0, inplace=True)
        dfraw['CALL_TYPE']=dfraw['CALL_TYPE'].astype('int')
        dfraw['IS_ROAMING']=dfraw['IS_ROAMING'].astype('int')
        dfraw['SERVICE_KEY']=dfraw['SERVICE_KEY'].astype('int')
        dfraw['DIAMETER_RESULT_CODES']=dfraw['DIAMETER_RESULT_CODES'].astype('int')
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
        rawsuc=dfraw[dfraw['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawmocsuc=dfrawmoc[dfrawmoc['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawmtcsuc=dfrawmtc[dfrawmtc['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawmfcsuc=dfrawmfc[dfrawmfc['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawroamsuc=dfrawroam[dfrawroam['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawnonroamsuc=dfrawnonroam[dfrawnonroam['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        raw100suc=dfraw100[dfraw100['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        raw150suc=dfraw150[dfraw150['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        raw200suc=dfraw200[dfraw200['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        raw300suc=dfraw300[dfraw300['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawmmsuc=dfrawmm[dfrawmm['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        rawpksuc=dfrawpk[dfrawpk['DIAMETER_RESULT_CODES'].isin(list_diameter)]
        


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
        for c in scp_colum:
            if c not in join_column:
                joinfinal[c]=0
        dffinal=joinfinal[scp_colum]
        dffinal.fillna(0, inplace=True)
        dffinal.to_csv(outputfile,index=False)


def JoinDataScp():
    pathdir=os.getcwd()
    olddata=f'{pathdir}/rawdata/scp_data_minute.csv'
    newdata=f'{pathdir}/rawdata/scp_newdata_minute.csv'
    raw=[]
    try :
        dfold=pd.read_csv(olddata)
    except Exception :
        dfold=pd.DataFrame(raw,columns=scp_colum)
    dfnew=pd.read_csv(newdata)
    print(dfnew)
    dfnew=dfnew[scp_colum]
    if len(dfold.index) > 0 : #check old data exist
        list_oldhour=[]
        dictold=dfold.to_dict('records')
        dictnew=dfnew.to_dict('records')   
        [list_oldhour.append(t['CDR_HOUR']) for t in dictold] 
        for dn in dictnew:
            if dn['CDR_HOUR'] in list_oldhour: #check new data in old data
                    index = list_oldhour.index(dn['CDR_HOUR'])
                    for k in dictold[index].keys():
                        if k != 'CDR_HOUR' or k != 'index' :
                            if dictold[index][k] > dictold[index][k]:
                                dictold[index][k]=dn[k]
            else : #check new data in old data
                dictold.append(dn)
        dfcombine=pd.DataFrame(dictold)
        list_hour=dfcombine['CDR_HOUR'].tolist()
        list_clean=list_hour[-24:]
        dfclean=dfcombine[dfcombine['CDR_HOUR'].isin(list_clean)]
        dfclean.to_csv(olddata,index=False)
    else : #check old data exist
        dfnew.to_csv(olddata,index=False)


def SdpProcessMinute():
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/sdp_newdata_minute.csv'
    try :
        dfraw=pd.read_csv(inputsdp)
        flag=1
    except Exception :
        pass
    if flag == 1 :
        #new data raw
        dfraw.fillna(0, inplace=True)
        dfraw['ACCESSFLAG']=dfraw['ACCESSFLAG'].astype('int')
        dfraw['TOTAL']=dfraw['TOTAL'].astype('int')

        #filter data raw
        dfrawbmo=dfraw[dfraw['ACCESSFLAG']== 66 ]
        dfrawbmt=dfraw[dfraw['ACCESSFLAG']== 67 ]
        dfrawdig=dfraw[dfraw['ACCESSFLAG']== 68 ]
        dfrawsmo=dfraw[dfraw['ACCESSFLAG']== 72 ]
        dfrawsmt=dfraw[dfraw['ACCESSFLAG']== 73 ]
        rawsuc=dfraw[dfraw['INTERNALCAUSE'].isin(list_diameter)]
        rawbmosuc=dfrawbmo[dfrawbmo['INTERNALCAUSE'].isin(list_diameter)]
        rawbmtsuc=dfrawbmt[dfrawbmt['INTERNALCAUSE'].isin(list_diameter)]
        rawdigsuc=dfrawdig[dfrawdig['INTERNALCAUSE'].isin(list_diameter)]
        rawsmosuc=dfrawsmo[dfrawsmo['INTERNALCAUSE'].isin(list_diameter)]
        rawsmtsuc=dfrawsmt[dfrawsmt['INTERNALCAUSE'].isin(list_diameter)]

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


def JoinDataSdp():
    pathdir=os.getcwd()
    olddata=f'{pathdir}/rawdata/sdp_data_minute.csv'
    newdata=f'{pathdir}/rawdata/sdp_newdata_minute.csv'
    raw=[]
    try :
        dfold=pd.read_csv(olddata)
    except Exception :
        dfold=pd.DataFrame(raw,columns=sdp_colum)
    dfnew=pd.read_csv(newdata)
    dfnew=dfnew[sdp_colum]
    if len(dfold.index) > 0 : #check old data exist
        list_oldhour=[]
        dictold=dfold.to_dict('records')
        dictnew=dfnew.to_dict('records')   
        [list_oldhour.append(t['CDR_HOUR']) for t in dictold] 
        for dn in dictnew:
            if dn['CDR_HOUR'] in list_oldhour: #check new data in old data
                    index = list_oldhour.index(dn['CDR_HOUR'])
                    for k in dictold[index].keys():
                        if k != 'CDR_HOUR' or k != 'index' :
                            if dictold[index][k] > dictold[index][k]:
                                dictold[index][k]=dn[k]
            else : #check new data in old data
                dictold.append(dn)
        dfcombine=pd.DataFrame(dictold)
        list_hour=dfcombine['CDR_HOUR'].tolist()
        list_clean=list_hour[-24:]
        dfclean=dfcombine[dfcombine['CDR_HOUR'].isin(list_clean)]
        dfclean.to_csv(olddata,index=False)
    else : #check old data exist
        dfnew.to_csv(olddata,index=False)


SftpFile()
ScpProcessMinute()
JoinDataScp()
SdpProcessMinute()
JoinDataSdp()
