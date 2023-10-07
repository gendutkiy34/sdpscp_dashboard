import json
import os
import cx_Oracle
from modules.general import ReadJsonFile

basedir=os.path.abspath(os.path.dirname(__file__))

def OracleCon(patfile):
    json_cred=ReadJsonFile(patfile)
    username=json_cred['username']
    password=json_cred['password']
    host=json_cred['host']
    sid=json_cred['sid']
    port=json_cred['port']
    try :
        connection = cx_Oracle.connect(user=username, password=password,
                               dsn="{0}:{1}/{2}".format(host,port,sid),
                               encoding="UTF-8")
    except Exception :
        connection="connection failed !!!!"  
    return connection

def OracleConPd(patfile):
    json_cred=ReadJsonFile(patfile)
    username=json_cred['username']
    password=json_cred['password']
    host=json_cred['host']
    sid=json_cred['sid']
    port=json_cred['port']
    try :
        connection = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(password,username,host,port,sid))
    except Exception :
        connection="connection failed !!!!"  
    return connection
