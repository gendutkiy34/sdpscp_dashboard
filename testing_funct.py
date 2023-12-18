import pandas as pd
import os
import json
import time
from datetime import datetime,timedelta
from modules.general import GetToday


pd.options.mode.chained_assignment = None

import pandas as pd
import time
import os
import schedule
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from GrepDataNew import GetDataMinute

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
       '300SUC_0','300SUC_1','300SUC_7']

sdp_colum=['CDR_HOUR', 'ATT_0', 'ATT_1', 'ATT_7', 'SUC_0', 'SUC_1', 'SUC_7',
       'BMOATT_0', 'BMOATT_1', 'BMOATT_7', 'BMOSUC_0', 'BMOSUC_1', 'BMOSUC_7',
       'BMTATT_0', 'BMTATT_1', 'BMTATT_7', 'BMTSUC_0', 'BMTSUC_1', 'BMTSUC_7',
       'DIGATT_0_x', 'DIGATT_1_x', 'DIGATT_7_x', 'DIGATT_0_y', 'DIGATT_1_y',
       'DIGATT_7_y', 'SMOATT_0', 'SMOATT_1', 'SMOATT_7', 'SMOSUC_0',
       'SMOSUC_1', 'SMOSUC_7', 'SMTATT_0', 'SMTATT_1', 'SMTATT_7', 'SMTSUC_0',
       'SMTSUC_1', 'SMTSUC_7', ]
#'RNWATT_0', 'RNWATT_1', 'RNWATT_7', 'RNWSUC_0','RNWSUC_1', 'RNWSUC_7'

def ScpMinute():
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/scp_newdata_minute.csv'
    conpath=(f'{pathdir}/connections/scpprodtrx.json')
    sqltxt=ReadTxtFile(f'{pathdir}/sql/scpminute2.sql')
    today=GetToday()
    list_day=[0,1,7]
    list_newdata=[]
    for dy in list_day:
        dt=today - timedelta(days=dy)
        data=GetDataMinute(pathsql=sqltxt,pathconnection=conpath,dat=dt)
        df=pd.DataFrame(data)
        df['REMARK']=f'day{dy}'
        datanew=df.to_dict('records')
        for dn in datanew:
            list_newdata.append(dn)
    #new data raw
    dfraw=pd.DataFrame(list_newdata)
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
    print(dfraw200.info())
    dfraw300=dfraw[dfraw['SERVICE_KEY']== 300 ]
    rawsuc=dfraw[dfraw['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    rawmocsuc=dfrawmoc[dfrawmoc['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    rawmtcsuc=dfrawmtc[dfrawmtc['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    rawmfcsuc=dfrawmfc[dfrawmfc['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    rawroamsuc=dfrawroam[dfrawroam['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    rawnonroamsuc=dfrawnonroam[dfrawnonroam['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    raw100suc=dfraw100[dfraw100['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    raw150suc=dfraw150[dfraw150['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    raw200suc=dfraw200[dfraw200['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    raw300suc=dfraw300[dfraw300['DIAMETER_RESULT_CODES'].isin(['2001','4010','4012','5030','5031'])]
    print(raw200suc.info())

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
    print(df200att)
    df200suc=pd.pivot_table(raw200suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
    df200suc=df200suc.rename(columns={'day0':'200SUC_0','day1':'200SUC_1','day7':'200SUC_7'})
    print(df200suc)
    df300att=pd.pivot_table(dfraw300,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
    df300att=df300att.rename(columns={'day0':'300ATT_0','day1':'300ATT_1','day7':'300ATT_7'})
    df300suc=pd.pivot_table(raw300suc,values="TOTAL",index=["CDR_HOUR"],columns=["REMARK"],aggfunc={'TOTAL': "sum"}).reset_index()
    df300suc=df300suc.rename(columns={'day0':'300SUC_0','day1':'300SUC_1','day7':'300SUC_7'})

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
    print(dfjoin9[['CDR_HOUR','200ATT_0','200ATT_1','200ATT_7','200SUC_0','200SUC_1','200SUC_7']])
    joinfinal=pd.merge(dfjoin9,df300suc,on=['CDR_HOUR'],how='left').reset_index()
    print(joinfinal.columns)
    print(joinfinal[['CDR_HOUR','200ATT_0','200ATT_1','200ATT_7','200SUC_0','200SUC_1','200SUC_7']])
    join_column=joinfinal.columns
    for c in scp_colum:
        if c not in join_column:
            joinfinal[c]=0
    dffinal=joinfinal[scp_colum]
    print(dffinal[['CDR_HOUR','200ATT_0','200ATT_1','200ATT_7','200SUC_0','200SUC_1','200SUC_7']])


ScpMinute()