import pandas as pd
import time
import os
import schedule
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list

#variable
path=os.getcwd()
remote_dir='/home/sdpuser/TechM_Sdp/output/'
inputscp='{0}/rawdata/scp_raw_minute.csv'.format(path)
inputsdp='{0}/rawdata/sdp_raw_minute.csv'.format(path)
col_scp=['CDR_HOUR','ATT_MM','BFT_MM','BYP_MM','ATT_PK','BFT_PK','BYP_PK']
col_sdp=['CDR_HOUR', 'bulk_mo', 'bulk_mt', 'dig_srv', 'subs_mo', 'subs_mt']




def ScpRawProccess():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/scp_data_realtime.csv'
    df_old=None

    #open old data
    try :
        df_old=pd.read_csv(outputfile)
    except Exception :
        pass
    
    #process new data raw
    raw_scp=pd.read_csv(inputscp)
    #--- filter data raw 
    dfmm=raw_scp[raw_scp['INSTANCE_ID']=='MM']
    dfpk=raw_scp[raw_scp['INSTANCE_ID']=='PK']
    rwbftmm=dfmm[dfmm['ISBFT']== 1]
    rwbftpk=dfpk[dfpk['ISBFT']== 1]
    rwbyppmm=dfmm[dfmm['IS_CHARGING_OVERRULED']== 1]
    rwbyppk=dfpk[dfpk['IS_CHARGING_OVERRULED']== 1]

    #--- pivot data raw
    attmm=pd.pivot_table(dfmm,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    attmm=attmm.rename(columns={'MM':'ATT_MM'})
    bftmm=pd.pivot_table(rwbftmm,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    bftmm=bftmm.rename(columns={'MM':'BFT_MM'})
    bypmm=pd.pivot_table(rwbyppmm,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    bypmm=bypmm.rename(columns={'MM':'BYP_MM'})
    attpk=pd.pivot_table(dfpk,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    attpk=attpk.rename(columns={'PK':'ATT_PK'})
    bftpk=pd.pivot_table(rwbftpk,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    bftpk=bftpk.rename(columns={'PK':'BFT_PK'})
    byppk=pd.pivot_table(rwbyppk,values="TOTAL",index=["CDR_HOUR"],columns=["INSTANCE_ID"],aggfunc={'TOTAL': "sum"}).reset_index()
    byppk=byppk.rename(columns={'PK':'BYP_PK'})

    #---join
    dfjoin1=pd.merge(pd.merge(attmm,bftmm,on=['CDR_HOUR'],how='left'),bypmm,on=['CDR_HOUR'],how='left')
    dfjoin2=pd.merge(pd.merge(dfjoin1,attpk,on=['CDR_HOUR'],how='left'),bftpk,on=['CDR_HOUR'],how='left')
    df_new=pd.merge(dfjoin2,byppk,on=['CDR_HOUR'],how='left')
    for c in col_scp:
        if c in df_new.columns :
            pass
        else :
            df_new[c]=0
    df_new=df_new[col_scp]

    #combine olddata & newdata
    if df_old is not None  :
        list_oldhour=[]
        dict_old=df_old.to_dict('records')
        dict_new=df_new.to_dict('records')
        [list_oldhour.append(t['CDR_HOUR']) for t in dict_old] 
        for dn in dict_new:
            if dn['CDR_HOUR'] in list_oldhour: #check new data in old data 
                index = list_oldhour.index(dn['CDR_HOUR'])
                for k in dict_old[index].keys():
                    if k != 'CDR_HOUR' or k != 'index' :
                        if dict_old[index][k] > dict_old[index][k]:
                            dict_old[index][k]=dn[k]
            else : #check new data in old data
                dict_old.append(dn)
        dfcombine=pd.DataFrame(dict_old)
        list_hour=dfcombine['CDR_HOUR'].tolist()
        list_clean=list_hour[-15:]
        dfclean=dfcombine[dfcombine['CDR_HOUR'].isin(list_clean)]
        dfclean.fillna(0,inplace=True)
        dfclean.to_csv(outputfile,index=False)
    else : #check old data exist
        df_new.to_csv(outputfile,index=False)


def SdpRawProccess():
    flag=0
    pathdir=os.getcwd()
    outputfile=f'{pathdir}/rawdata/sdp_data_realtime.csv'
    df_old=None

    #open old data
    try :
        df_old=pd.read_csv(outputfile)
    except Exception :
        pass
    
    #process new data raw
    raw_sdp=pd.read_csv(inputsdp)
    #--- pivot
    df_new=pd.pivot_table(raw_sdp,values="TOTAL",index=["CDR_HOUR"],columns=["ACCESSFLAG"],aggfunc={'TOTAL': "sum"}).reset_index()
    df_new=df_new.rename(columns={66:'bulk_mo',67:'bulk_mt',68:'dig_srv',72:'subs_mo',73:'subs_mt'})
    for c in col_sdp:
        if c in df_new.columns :
            pass
        else :
            df_new[c]=0
    df_new=df_new[col_sdp]

    #combine olddata & newdata
    if df_old is not None  :
        list_oldhour=[]
        dict_old=df_old.to_dict('records')
        dict_new=df_new.to_dict('records')
        [list_oldhour.append(t['CDR_HOUR']) for t in dict_old]
        for dn in dict_new:
            if dn['CDR_HOUR'] in list_oldhour: #check new data in old data 
                index = list_oldhour.index(dn['CDR_HOUR'])
                for k in dict_old[index].keys():
                    if k != 'CDR_HOUR' or k != 'index' :
                        if dict_old[index][k] > dict_old[index][k]:
                            dict_old[index][k]=dn[k]
            else : #check new data in old data
                dict_old.append(dn)
        dfcombine=pd.DataFrame(dict_old)
        list_hour=dfcombine['CDR_HOUR'].tolist()
        list_clean=list_hour[-15:]
        dfclean=dfcombine[dfcombine['CDR_HOUR'].isin(list_clean)]
        dfclean.fillna(0,inplace=True)
        dfclean.to_csv(outputfile,index=False)
    else : #check old data exist
        df_new.to_csv(outputfile,index=False)

ScpRawProccess()
SdpRawProccess()