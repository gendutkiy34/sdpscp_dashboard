import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData
import pandas as pd


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()


@app.route('/')
def index():
    #variable
    rawscp='./rawdata/scp_data_raw.csv'
    rawsdp='./rawdata/sdp_data_raw.csv'
    #data scp 
    datascp=ScpData(pathfile=rawscp)
    #dfscptoday=datascp.VerifyData()
    scpatt,scpsuc,scpsr=datascp.SumDataToday()
    #print(f'num of data scp : {len(dfscptoday.index)}')
    print(f'voice attemp : {scpatt} , voice success : {scpsuc} , voice success rate : {scpsr} %')
    list_scp_att,list_scp_min=datascp.HourMinScp()
    #data sdp
    datasdp=SdpData(pathfile=rawsdp)
    #dfsdptoday=datasdp.VerifyData()
    #print(f'num of data scp : {len(dfsdptoday.index)}')
    bmtatt,bmtsuc,bmtsr=datasdp.SumDataToday(accflag=67)
    print(f'bulkmt attemp : {bmtatt} , bulkmt success : {bmtsuc} , bulkmt success rate : {bmtsr} %')
    bmoatt,bmosuc,bmosr=datasdp.SumDataToday(accflag=66)
    print(f'bulkmo attemp : {bmoatt} , bulkmo success : {bmosuc} , bulkmo success rate : {bmosr} %')
    digatt,digsuc,digsr=datasdp.SumDataToday(accflag=68)
    print(f'digital attemp : {digatt} , digital success : {digsuc} , digital success rate : {digsr} %')
    smtatt,smtsuc,smtsr=datasdp.SumDataToday(accflag=73)
    print(f'sbulkmt attemp : {smtatt} , sbulkmt success : {smtsuc} , sbulkmt success rate : {smtsr} %')
    smoatt,smosuc,smosr=datasdp.SumDataToday(accflag=73)
    print(f'sbulkmo attemp : {smoatt} , sbulkmo success : {smosuc} , sbulkmo success rate : {smosr} %')
    list_bmo,list_bmt,list_dig,list_smo,list_smt,list_sdp_min=datasdp.HourMinSdp()
    print(list_bmo)
    return render_template('dashboard.html',scpatt=scpatt,scpsr=scpsr,bmoatt=bmoatt,bmosr=bmosr,bmtatt=bmtatt,
                           bmtsr=bmtsr,digatt=digatt,digsr=digsr,smoatt=smoatt,smosr=smosr,smtatt=smtatt,
                           smtsr=smtsr,listscpmin=list_scp_min,listscpatt=list_scp_att,listsdpmin=list_sdp_min,
                           listbmoatt=list_bmo,listbmtatt=list_bmt,listdigatt=list_dig,listsmoatt=list_smo,
                           listsmtatt=list_smt)


@app.route('/scpcompare')
def scp3d():
    return render_template('dashboard_scp_compare_today.html')

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8081')
    #app.run(debug=True,port='8081')