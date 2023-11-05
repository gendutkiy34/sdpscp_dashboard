import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017
import pandas as pd


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()


@app.route('/')
def index():
    return render_template('dashboardV2design.html')

@app.route('/test2')
def test2():
    return render_template('dashboard_sdp_today .html')

@app.route('/scpd017')
def scpd017():
    return render_template('baseappV2.html')

@app.route('/testvar')
def testvar():
    pathdir=os.path.abspath(os.path.dirname(__file__))
    rawsdp=f'{pathdir}/rawdata/data_sdp_today.csv'
    datasdp=SdpData(rawsdp)
    listsr=[66,67,68,72,73]
    dic_data={}
    for s in listsr :
        att_label=f'att{s}'
        suc_label=f'suc{s}'
        sr_label=f'sr{s}'
        att,succ,hour=datasdp.AttHourToday(accflag=s)
        sr,srhour=datasdp.SucRatHourly(accflag=s)
        print(sr)
        dic_data[att_label]=att
        dic_data[suc_label]=succ    
        dic_data[sr_label]=sr 
    dic_data['hour']=hour
    return render_template('dashboard_teslayout2.html',dic_sdp=dic_data)

@app.route('/sdptoday')
def sdptoday():
    topcp={}
    pathdir=os.path.abspath(os.path.dirname(__file__))
    rawsdp=f'{pathdir}/rawdata/data_sdp_today.csv'
    datasdp=SdpData(rawsdp)
    listsr=[66,67,68,72,73]
    dic_data={}
    for s in listsr :
        att_label=f'att{s}'
        suc_label=f'suc{s}'
        sr_label=f'sr{s}'
        att,succ,hour=datasdp.AttHourToday(accflag=s)
        sr,srhour=datasdp.SucRatHourly(accflag=s)
        print(sr)
        dic_data[att_label]=att
        dic_data[suc_label]=succ    
        dic_data[sr_label]=sr 
    dic_data['hour']=hour
    cprev,rev=datasdp.RevTop5()
    topcp['cprev']=cprev
    topcp['revenue']=rev
    cpatt,att=datasdp.AttTop5()
    topcp['cpatt']=cpatt
    topcp['attempt']=att
    dfsum=datasdp.Summary()
    dic_data['summary']=dfsum
    return render_template('dashboard_sdp_today.html',dic_sdp=dic_data,dic_top=topcp)


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')