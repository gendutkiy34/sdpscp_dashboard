import pandas as pd
import numpy as np
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
inputscp='{0}/rawdata/dashboard_scp_trx_minute.csv'.format(path)
inputsdp='{0}/rawdata/dashboard_sdp_trx_minute.csv'.format(path)
outputfile=f'{path}/rawdata/data_realtime_minute.csv'
outputerr=f'{path}/rawdata/error_realtime_minute.csv'

col=['CDR_DATE', 'HOURMINUTE', 'MM', 'PK', 'BULK_MO', 'BULK_MT', 'DIGITAL_SERVICE', 'SUBSCRIPTIONBULK_MO', 'SUBSCRIPTIONBULK_MT']
column_final=['CDR_DATE', 'HOURMINUTE', 'err83', 'err601', 'billing_timeout',
                        'err5000', 'err5004', 'err5005', 'err5012', 'err6000']

list_diameter=[2001,4010,4012,5030,5031]

def SftpFile():
    pathdir=os.getcwd()
    creden=f'{pathdir}/connections/mgwsit.json'
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}dashboard_scp_trx_minute.csv',localdir=inputscp)
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}dashboard_sdp_trx_minute.csv',localdir=inputsdp)


def NewDataProcess():
    flag=0
    pathdir=os.getcwd()
    print(inputscp)
    print(inputsdp)
    try :
        rawscp=pd.read_csv(inputscp)
        rawsdp=pd.read_csv(inputsdp)
        flag=1
    except Exception :
        pass
    if flag == 1 :
        df_sdp=pd.pivot_table(rawsdp,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['NETWORKMODE']
                      ,aggfunc="sum", fill_value=0).reset_index()
        df_scp=pd.pivot_table(rawscp,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['NODE']
                      ,aggfunc="sum", fill_value=0).reset_index()
        df_merge=pd.merge(df_scp,df_sdp,how='inner',on=['CDR_DATE','HOURMINUTE'])
        df_merge.sort_values(by='HOURMINUTE',ascending=False,inplace=True)
        return df_merge[col]
        
def NewErrorProcess():
    flag=0
    pathdir=os.getcwd()
    print(inputscp)
    print(inputsdp)
    try :
        raw_scp=pd.read_csv(inputscp)
        raw_sdp=pd.read_csv(inputsdp)
        flag=1
    except Exception :
        pass
    if flag == 1 :
        #proccess SDP new 
        list_col=['CDR_DATE','HOURMINUTE','err83','err601','billing_timeout']
        base_df=raw_sdp[['CDR_DATE','HOURMINUTE']].drop_duplicates()
        raw83=raw_sdp[raw_sdp['BASICCAUSE']==83]
        raw601=raw_sdp[raw_sdp['BASICCAUSE']==601]
        rawbilling=raw_sdp[(raw_sdp['BASICCAUSE'].isin([940,950,948])) & (~raw_sdp['INTERNALCAUSE'].notnull())]
        pivot83=pd.pivot_table(raw83,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['BASICCAUSE']
                      ,aggfunc="sum", fill_value=0).reset_index()
        pivot83.rename(columns={'BASICCAUSE':'err83',83:'err83'},inplace=True)
        pivot601=pd.pivot_table(raw601,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['BASICCAUSE']
                      ,aggfunc="sum", fill_value=0).reset_index()        
        pivot601.rename(columns={'BASICCAUSE':'err601',601:'err601'},inplace=True)            
        pivotbilling=pd.pivot_table(rawbilling,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['BASICCAUSE']
                      ,aggfunc="sum", fill_value=0).reset_index()   
        pivotbilling.rename(columns={'BASICCAUSE':'billing_timeout','83':'err83'},inplace=True)   
        df_merge1=pd.merge(base_df,pivot83,how='left',on=['CDR_DATE','HOURMINUTE'])
        df_merge2=pd.merge(df_merge1,pivot601,how='left',on=['CDR_DATE','HOURMINUTE'])
        df_merge3=pd.merge(df_merge2,pivotbilling,how='left',on=['CDR_DATE','HOURMINUTE'])
        for c in list_col :
            if c not in df_merge3.columns:
                df_merge3[c]=np.nan
        sdp_final=df_merge3[list_col].fillna(0)
        sdp_final=sdp_final.astype({'err83': 'int', 'err601': 'int', 'billing_timeout': 'int'})

        #proccess SCP new 
        scp_col=['CDR_DATE','HOURMINUTE','err5000','err5004','err5005','err5012','err6000']
        raw_bft=raw_scp[raw_scp['ISBFT']==1]
        raw_bft['DIAMETER_RESULT_CODES']=raw_bft['DIAMETER_RESULT_CODES'].fillna(0)
        raw_bft=raw_bft.astype({"DIAMETER_RESULT_CODES":"int"})
        pivotbft=pd.pivot_table(raw_bft,values='TOTAL',index=['CDR_DATE','HOURMINUTE'],columns=['DIAMETER_RESULT_CODES']
                      ,aggfunc="sum", fill_value=0).reset_index()
        pivotbft.rename(columns={5000:'err5000',5004:'err5004',5005:'err5005',5012:'err5012',6000:'err6000'},inplace=True)
        for c in scp_col :
            if c not in pivotbft.columns:
                pivotbft[c]=np.nan  
        scp_final=pivotbft[scp_col]

        #merge sdp_final &  scp_final
        df_merge1=pd.merge(sdp_final,scp_final,how='left',on=['CDR_DATE','HOURMINUTE'])
        df_final=df_merge1.fillna(0)
        for c in column_final[2:]:
            df_final[c]=df_final[c].astype('int')
        return df_final[column_final]

def CompareDataNewOld():
    pathdir=os.getcwd()
    newraw=NewDataProcess()
    newraw['CDR_DATE']=pd.to_datetime(newraw['CDR_DATE'], format='%d-%m-%Y').dt.date
    try :
        tempraw=pd.read_csv(outputfile)
        list_hour=tempraw['HOURMINUTE'].values.tolist()
        tempraw['CDR_DATE']=pd.to_datetime(tempraw['CDR_DATE'], format='%Y-%m-%d').dt.date 
        list_date=tempraw['CDR_DATE'].values.tolist()
        oldraw=tempraw[col]
    except :
        oldraw=None
    if oldraw is not None :
        list_newdata=[]
        for dn in newraw.iterrows():
            if dn[1]['CDR_DATE'] in list_date and dn[1]['HOURMINUTE'] in list_hour :
                for c in ['MM', 'PK', 'BULK_MO', 'BULK_MT', 'DIGITAL_SERVICE','SUBSCRIPTIONBULK_MT','SUBSCRIPTIONBULK_MO'] :
                    oldraw[c]=oldraw[c].astype(int)
                    oldraw.loc[(oldraw['CDR_DATE'] == dn[1]['CDR_DATE'] ) &
                           (oldraw['HOURMINUTE'] == dn[1]['HOURMINUTE'] ) &
                           (oldraw[c] < int(dn[1][c])),c] = dn[1][c]
            else :
                listtemp=list(dn[1])
                list_newdata.append(listtemp)
        df_new=pd.DataFrame(list_newdata,columns=col)
        df_final=pd.concat([oldraw,df_new])
    else :
        df_final=newraw
    df_final['CDR_DATE']=pd.to_datetime(df_final['CDR_DATE'], format='%Y-%m-%d').dt.date
    df_result=df_final.sort_values(by=['CDR_DATE','HOURMINUTE'],ascending=False)
    df_result[col][:17].to_csv(outputfile,index=False)


def CompareErrorNewOld():
    pathdir=os.getcwd()
    newraw=NewErrorProcess()
    newraw['CDR_DATE']=pd.to_datetime(newraw['CDR_DATE'], format='%d-%m-%Y').dt.date
    try :
        tempraw=pd.read_csv(outputerr)
        list_hour=tempraw['HOURMINUTE'].values.tolist()
        tempraw['CDR_DATE']=pd.to_datetime(tempraw['CDR_DATE'], format='%Y-%m-%d').dt.date 
        list_date=tempraw['CDR_DATE'].values.tolist()
        oldraw=tempraw[col]
    except :
        oldraw=None
    if oldraw is not None :
        list_newdata=[]
        for dn in newraw.iterrows():
            if dn[1]['CDR_DATE'] in list_date and dn[1]['HOURMINUTE'] in list_hour :
                for c in [ 'err83', 'err601', 'billing_timeout','err5000', 'err5004', 'err5005', 'err5012', 'err6000'] :
                    oldraw[c]=oldraw[c].astype(int)
                    oldraw.loc[(oldraw['CDR_DATE'] == dn[1]['CDR_DATE'] ) &
                           (oldraw['HOURMINUTE'] == dn[1]['HOURMINUTE'] ) &
                           (oldraw[c] < int(dn[1][c])),c] = dn[1][c]
            else :
                listtemp=list(dn[1])
                list_newdata.append(listtemp)
        df_new=pd.DataFrame(list_newdata,columns=column_final)
        df_final=pd.concat([oldraw,df_new])
    else :
        df_final=newraw
    df_final['CDR_DATE']=pd.to_datetime(df_final['CDR_DATE'], format='%Y-%m-%d').dt.date
    df_result=df_final.sort_values(by=['CDR_DATE','HOURMINUTE'],ascending=False)
    df_result[column_final][:17].to_csv(outputerr,index=False)


SftpFile()
NewDataProcess()
NewErrorProcess()
CompareDataNewOld()
CompareErrorNewOld()

