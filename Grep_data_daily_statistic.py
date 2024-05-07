import pandas as pd
import time
import os
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.remoteclient import GetRemoteFile,SshNode


#variable
now=GetToday()
ytrd=now-timedelta(days=1)
dtstr=ConvertDatetoStr(tgl=ytrd,format='%Y%m')
path=os.getcwd()
remote_dir='/home/sdpuser/TechM_Sdp/output/'
inputscp='{0}/rawdata/scp_statistic_daily_hour.csv'.format(path)
inputsdp='{0}/rawdata/sdp_statistic_daily_hour.csv'.format(path)
outputscp=f'/home/scpsdpdev/data_statistic/scp_statistic_{dtstr}.csv'.format(path)
outputsdp=f'/home/scpsdpdev/data_statistic/sdp_statistic_{dtstr}.csv'.format(path)


def SftpFile():
    pathdir=os.getcwd()
    creden=f'{pathdir}/connections/mgwsit.json'
    SshNode(pathcred=creden,comm='python /home/sdpuser/TechM_Sdp/Scripts/Grep_data_statistic_daily.py')
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}scp_statistic_daily_hour.csv',localdir=inputscp)
    GetRemoteFile(pathcred=creden,remotedir=f'{remote_dir}sdp_statistic_daily_hour.csv',localdir=inputsdp)


def JoinDataOldNew(env=None):
    flag=0
    olddata=None
    newdata=None
    pathdir=os.getcwd()
    if env == 'scp' :
        if os.path.isfile(outputscp) :
            olddata=pd.read_csv(outputscp)
            os.system(f'echo "total old_data  scp {len(olddata.index)}"')
        if os.path.isfile(inputscp) :
            newdata=pd.read_csv(inputscp)
            os.system(f'echo "total new_data scp {len(newdata.index)}"')
        outputfile=outputscp
    else :
        if os.path.isfile(outputsdp) :
            olddata=pd.read_csv(outputsdp)
            os.system(f'echo "total old_data  sdp {len(olddata.index)}"')
        if os.path.isfile(inputsdp) :
            newdata=pd.read_csv(inputsdp)
            os.system(f'echo "total old_data  sdp {len(olddata.index)}"')
        outputfile=outputsdp
    if newdata is not None :
        if olddata is not None :
            dfjoin=pd.concat([olddata,newdata])
            dfjoin.to_csv(outputfile,index=False)
            os.system(f'echo "total join  {len(dfjoin.index)}"')
        else :
            newdata.to_csv(outputfile,index=False)
            os.system(f'echo "no join data"')


SftpFile()
JoinDataOldNew(env='scp')
JoinDataOldNew(env='sdp')
os.system('dt=`date` ; echo  "$dt -- job finish!"')
