import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict
from formapp import FormHttpReq,FormLog,FormCdr,FormCpId
from modules.htttprequest import ReqHttp
from modules.scplog import GetScpLog,ExtractScpLog
from modules.sdplog import GetSdpLog
from modules.ReadSdpLog import ExtractScmLog
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday
from modules.general import ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.extractcdr import ExtractCdrSdp


today=GetToday()
dt_string=ConvertDatetoStr(today,'%Y-%m-%d')
dbscp=('./connections/scpprodtrx.json')
dbsdp=('./connections/sdpprodtrx.json')
sqlscp=ReadTxtFile('./sql/scphourlytoday.sql')
sqlbulk=ReadTxtFile('./sql/bulkservicetoday.sql')
sqldig=ReadTxtFile('./sql/digitalservicetoday.sql')
sqlsubs=ReadTxtFile('./sql/subscriptionservicetoday.sql')
data_scp=GetDataToday(conpath=dbscp,tgl=dt_string,cdrtype='scp',sqlraw=sqlscp)
print(isinstance(data_scp,str))

