import os
import paramiko
import json


def Credential(credential=None):
    if credential is not None :
        with open(credential,'r') as f:
            cred=json.load(f)
    return cred

def SshNode(pathcred=None,comm=None):
    if pathcred is not None :
        cred=Credential(credential=pathcred)
        client =paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try :
            client.connect(hostname=cred['host'],username=cred['username'],password=cred['password'])
            stdin,stdout,stderr=client.exec_command(comm)
            return stdout.read().decode()
            client.close()
        except Exception :
            pass

def GetRemoteFile(pathcred=None,remotedir=None,localdir=None):
    if pathcred is not None :
        cred=Credential(credential=pathcred)
        client =paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try :
            client.connect(hostname=cred['host'],username=cred['username'],password=cred['password'])
            sftp=client.open_sftp()
            sftp.get(remotedir, localdir)
            sftp.close()
            client.close()
        except Exception :
            pass



