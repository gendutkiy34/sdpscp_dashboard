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


@app.route('/')
def index():
    return render_template('baseappV2.html')


@app.route('/scptoday')
def scptoday():
    data_scp={}
    listhour=[]
    pathdir=os.path.abspath(os.path.dirname(__file__))
    rawsdp=f'{pathdir}/rawdata/scp_data_d017.csv'
    datascp=ScpDataD017(rawsdp)
    att,suc,sr,hou=datascp.HourlyDataToday()
    data_scp['listatt']=att
    data_scp['listsuc']=suc
    data_scp['listsr']=sr
    scpatt,scpsuc,scpsr,listdia,listsum=datascp.SumDataToday()
    data_scp['lissum']=listsum
    data_scp['lisdiameter']=listdia
    roamatt,roamsuc,roamsr,hou=datascp.HourlyDataToday(roaming=1)
    data_scp['listroamsr']=roamsr
    noroamatt,noroamsuc,noroamsr,hou=datascp.HourlyDataToday(roaming=1)
    data_scp['listnoroamsr']=noroamsr
    for h in hou:
        if h < 10 :
            listhour.append(f'0{h}')
        else :
            listhour.append(h)
    data_scp['list_hour']=listhour
    listhour,listbft,errbft,totalerrbft=datascp.BftToday()
    data_scp['listbft']=listbft
    data_scp['listerrbft']=errbft
    data_scp['listtotalbft']=totalerrbft
    for sk in list_sk:
        listatt=f'listatt{sk}'
        listsuc=f'listsuc{sk}'
        sumatt=f'sumatt{sk}'
        sumsuc=f'sumsuc{sk}'
        lisatt,sumt=datascp.AttSkToday(servicekey=sk)
        lissuc,sums=datascp.AttSkToday(servicekey=sk,diameter=2001)
        data_scp[listatt]=lisatt
        data_scp[sumatt]=sumt
        data_scp[listsuc]=lissuc
        data_scp[sumsuc]=sums
    attroam,sumroam=datascp.AttRoamToday(roaming=1)
    data_scp['roaming_att']=attroam
    data_scp['roaming_attsum']=sumroam
    sucroam,sumsroam=datascp.AttRoamToday(roaming=1,diameter=2001)
    data_scp['roaming_suc']=sucroam
    data_scp['roaming_sucsum']=sumsroam
    attnonroam,sumnonroam=datascp.AttRoamToday(roaming=0)
    data_scp['nonroaming_att']=attnonroam
    data_scp['nonroaming_attsum']=sumnonroam
    sucnonroam,sumsnonroam=datascp.AttRoamToday(roaming=0,diameter=2001)
    data_scp['nonroaming_suc']=sucnonroam
    data_scp['nonroaming_sucsum']=sumsnonroam
    print(data_scp)
    return render_template('dashboard_scp_today.html',dic_scp=data_scp)

@app.route('/sdptoday')
def sdptoday():
    return render_template('baseappV2.html')

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



if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')