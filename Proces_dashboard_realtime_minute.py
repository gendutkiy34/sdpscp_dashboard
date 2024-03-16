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
inputscp='{0}/rawdata/dashboard_scp_trx_minute.csv'.format(path)
inputsdp='{0}/rawdata/dashboard_sdp_trx_minute.csv'.format(path)
outputfile=f'{path}/rawdata/data_realtime_minute.csv'

col=['CDR_DATE', 'HOURMINUTE', 'MM', 'PK', 'BULK_MO', 'BULK_MT', 'DIGITAL_SERVICE', 'SUBSCRIPTIONBULK_MO', 'SUBSCRIPTIONBULK_MT']


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


SftpFile()
NewDataProcess()
CompareDataNewOld()
