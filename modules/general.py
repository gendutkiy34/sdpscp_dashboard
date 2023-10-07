import json
import os
import paramiko
from datetime import datetime

basedir=os.path.abspath(os.path.dirname(__file__))

def ReadTxtFile(pathfile):
    with open(pathfile,'r') as f:
        data=f.read()
    return data

def ReadJsonFile(pathfile):
    with open(pathfile,'r') as f:
        data=json.load(f)
    return data

def ConvertStrtoDate(tgl,format):
    dt=datetime.strptime(tgl,format)
    return dt

def ConvertDatetoStr(tgl,format):
    dt=datetime.strftime(tgl,format)
    return dt

def GetToday():
    dt=datetime.now()
    return dt

def SshNode(host=None,user=None,pwd=None,cmd=None):
    client =paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host,username=user,password=pwd)
    stdin,stdout,stderr=client.exec_command(cmd)
    return stdout,stderr
    client.close()

def ConvertListToDict(listkey=None,listvalue=None):
    list_item=[]
    for v in listvalue:
        item={}
        for k,val in zip(listkey,v):
            item[k.strip()]=val
        list_item.append(item)
    return list_item

def Sum2list(list1=None,list2=None):
    list_sum=[]
    for a,b in zip(list1,list2):
        c=a+b
        list_sum.append(c)
    return list_sum