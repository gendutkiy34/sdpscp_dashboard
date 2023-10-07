import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from formapp import FormHttpReq,FormLog,FormCdr,FormCpId
from modules.htttprequest import ReqHttp
from modules.scplog import GetScpLog,ExtractScpLog
from modules.sdplog import GetSdpLog
from modules.ReadSdpLog import ExtractScmLog
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate
from modules.extractcdr import ExtractCdrSdp
from getdataraw import GrepNewData
import pandas as pd


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()


@app.route('/')
def index():
    GrepNewData()
    try :
        hourlyscp=pd.read_csv('./rawdata/scp_today_hourly.csv')
        minutscp=pd.read_csv('./rawdata/scp_today_minute.csv')
        hourlysdp=pd.read_csv('./rawdata/sdp_today_hourly.csv')
        minutsdp=pd.read_csv('./rawdata/sdp_today_minute.csv')
    except Exception :
        hourlyscp=None
        hourlysdp=None
        minutscp=None
        minutsdp=None
    if hourlyscp is not None :
        scpatt=hourlyscp['ATTEMPT'].sum()
        scpsr=round((hourlyscp['SUCCESS'].sum()/scpatt)*100,2)
        tgl=hourlyscp['CDR_DATE'].drop_duplicates()[0]
    else :
        scpatt,scpsr='N/A'
    if hourlysdp is not None :
        list_sdp=[]
        list_attempt=['BMO_ATTEMPT', 'BMT_ATTEMPT', 'DIG_ATTEMPT','SMO_ATTEMPT', 'SMT_ATTEMPT']
        list_success=['BMO_SUCCESS', 'BMT_SUCCESS','DIG_SUCCESS', 'SMO_SUCCESS', 'SMT_SUCCESS']
        list_sv=['BMO', 'BMT','DIG', 'SMO', 'SMT']
        for a,s,sv in zip(list_attempt,list_success,list_sv):
            item={}
            item['service']=sv
            item['attempt']=hourlysdp[a].sum()
            item['success_rate']=round((hourlysdp[s].sum()/hourlysdp[a].sum())*100,2)
            list_sdp.append(item)
        for d in list_sdp:
            if d['service'] == 'BMO':
                bmoatt=d['attempt']
                bmosr=d['success_rate']
            elif d['service'] == 'BMT':
                bmtatt=d['attempt']
                bmtsr=d['success_rate']
            elif d['service'] == 'DIG':
                digatt=d['attempt']
                digsr=d['success_rate']
            elif d['service'] == 'SMO':
                smoatt=d['attempt']
                smosr=d['success_rate']
            elif d['service'] == 'SMT':
                smtatt=d['attempt']
                smtsr=d['success_rate']
    else :
        bmoatt,bmosr,bmtatt,bmtsr,digatt,digsr,smoatt,smosr,smtatt,smtsr='N/A'
    if minutscp is not None :
        list_scp_min=minutscp['CDR_DATE'].tolist()
        list_scp_att=minutscp['ATTEMPT'].tolist()
    else :
        list_scp_min=[]
        list_scp_att=[]
    if minutsdp is not None :
        list_sdp_min=minutsdp['CDRDATE'].tolist()
        list_bmo=minutsdp['BMO_ATTEMPT'].tolist()
        list_bmt=minutsdp['BMT_ATTEMPT'].tolist()
        list_dig=minutsdp['DIG_ATTEMPT'].tolist()
        list_smo=minutsdp['SMO_ATTEMPT'].tolist()
        list_smt=minutsdp['SMT_ATTEMPT'].tolist()
    else :
        list_sdp_min=[]
        list_bmo=[]
        list_bmt=[]
        list_dig=[]
        list_smo=[]
        list_smt=[]
    return render_template('dashboard.html',bmoatt=bmoatt,bmosr=bmosr,bmtatt=bmtatt,bmtsr=bmtsr,digatt=digatt,
                           digsr=digsr,smoatt=smoatt,smosr=smosr,smtatt=smtatt,smtsr=smtsr,scpatt=scpatt,
                           scpsr=scpsr,date=tgl,listscpmin=list_scp_min,listscpatt=list_scp_att,listsdpmin=list_sdp_min,
                           listbmoatt=list_bmo,listbmtatt=list_bmt,listdigatt=list_dig,listsmoatt=list_smo,
                           listsmtatt=list_smt)


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8034')