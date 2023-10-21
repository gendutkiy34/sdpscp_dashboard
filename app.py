import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from formapp import FormHttpReq,FormLog,FormCdr,FormCpId
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate
import pandas as pd


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()


@app.route('/')
def index():
    try :
        hourlyscp=pd.read_csv('./rawdata/scp_today_hourly.csv')
        minutscp=pd.read_csv('./rawdata/scp_today_minute.csv')
        hourlysdp=pd.read_csv('./rawdata/sdp_today_hourly.csv')
        minutsdp=pd.read_csv('./rawdata/sdp_today_minute.csv')
        flagdata=1
    except Exception :
        hourlyscp=pd.DataFrame(columns=['A'])
        hourlysdp=pd.DataFrame(columns=['A'])
        minutscp=pd.DataFrame(columns=['A'])
        minutsdp=pd.DataFrame(columns=['A'])
        flagdata=0
    print('flag data : {0}'.format(flagdata))
    print('data hourly - scp : {0}, sdp : {1}'.format(len(hourlyscp.index),len(hourlysdp.index)))
    print('data minute - scp : {0}, sdp : {1}'.format(len(minutscp.index),len(minutsdp.index)))
    if len(hourlyscp.index) > 0:
        scpatt=hourlyscp['ATTEMPT'].sum()
        scpsr=round((hourlyscp['SUCCESS'].sum()/scpatt)*100,2)
        tgl=hourlyscp['CDR_DATE'].drop_duplicates()[0]
    else :
        tgl='N/A'
        scpatt='N/A'
        scpsr='N/A'
    print('date update : {0}'.format(tgl))
    print('scpatt : {0}, scpsr :{1}'.format(scpatt,scpsr))
    if len(hourlysdp.index) > 0:
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
        bmoatt='N/A'
        bmosr='N/A'
        bmtatt='N/A'
        bmtsr='N/A'
        digatt='N/A'
        digsr='N/A'
        smoatt='N/A'
        smosr='N/A'
        smtatt='N/A'
        smtsr='N/A'
    print('bmoatt : {0}, bmosr : {1}, bmtatt : {2}, bmtsr : {3}, digatt : {4}, smoatt : {5}, smosr : {6}, smtatt : {7}, smtsr : {8}'.format(bmoatt,bmosr,
                                                                                                                                            bmtatt,bmtsr,
                                                                                                                                            digatt,digsr,
                                                                                                                                            smoatt,smosr,
                                                                                                                                            smtatt,smtsr))
    if len(minutscp.index) > 0 :
        list_scp_min=minutscp['CDR_DATE'].tolist()
        list_scp_att=minutscp['ATTEMPT'].tolist()
    else :
        list_scp_min=[]
        list_scp_att=[]
    print('num of data scp minute : {0}, num of data scpatt : {1}'.format(len(list_scp_min),len(list_scp_att)))
    if len(minutsdp.index) > 0 :
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
    print('num of data bmo : {0}, num of data bmt : {1}, num of data dig : {2}, num of data smo : {3}, num of data smt : {4}'.format(len(list_bmo),len(list_bmt),
                                                                                                                                     len(list_dig),len(list_smo),
                                                                                                                                     len(list_smt)))
    return render_template('dashboard.html',bmoatt=bmoatt,bmosr=bmosr,bmtatt=bmtatt,bmtsr=bmtsr,digatt=digatt,
                           digsr=digsr,smoatt=smoatt,smosr=smosr,smtatt=smtatt,smtsr=smtsr,scpatt=scpatt,
                           scpsr=scpsr,date=tgl,listscpmin=list_scp_min,listscpatt=list_scp_att,listsdpmin=list_sdp_min,
                           listbmoatt=list_bmo,listbmtatt=list_bmt,listdigatt=list_dig,listsmoatt=list_smo,
                           listsmtatt=list_smt)


if __name__ == '__main__':
    #app.run(debug=True,host='0.0.0.0',port='8081')
    app.run(debug=True,port='8081')