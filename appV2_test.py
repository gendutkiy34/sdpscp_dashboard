import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017
import pandas as pd


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()

#variable
list_sk=[100,200,133,150,300,400]
listsr=[66,67,68,72,73]

rawd3scp='./rawdata/scp_data_d017.csv'

@app.route('/')
def index():
    return render_template('dashboard_teslayout2.html')


@app.route('/scptoday')
def scptoday():
    return render_template('baseappV2.html')

@app.route('/sdptoday')
def sdptoday():
    return render_template('baseappV2.html')

@app.route('/sdpd017')
def sdpd017():
    return render_template('baseappV2.html')

@app.route('/scpd017')
def scpd017():
    data_scp={}
    datascp=ScpDataD017(rawd3scp)
    sumall=datascp.Summary()
    data_scp['summaryall']=sumall
    attall=datascp.Att()
    attsuc2001=datascp.Att(diameter=2001)
    data_scp['attall']=attall
    data_scp['attsuc2001']=attsuc2001
    rmatt=datascp.Att(roaming=1)
    rmsuc2001=datascp.Att(roaming=1,diameter=2001)
    data_scp['rmatt']=rmatt
    data_scp['rmsuc2001']=rmsuc2001
    nrmatt=datascp.Att(roaming=0)
    nrmsuc2001=datascp.Att(roaming=0,diameter=2001)
    data_scp['nrmatt']=nrmatt
    data_scp['nrmsuc2001']=nrmsuc2001
    return render_template('dashboard_scpd017.html',dict_scp=data_scp)


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



if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')