import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
app.config["SECRET_KEY"] = 'tO$and!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()

#variable
listaccflag=[66,67,68,72,73]
list_sk=[100,200,133,150,300,400]

#sourceinput
minscp='./rawdata/scp_data_minute.csv'
minsdp='./rawdata/sdp_data_minute.csv'
hourscp='./rawdata/scp_newdata_hour.csv'
hoursdp='./rawdata/sdp_newdata_hour.csv'
errfl='./rawdata/sdpscp_error_monitor.csv'

def bgError(data):
    if int(data) == 0 :
        color='cell_bg_ok'
    elif int(data) > 0 and int(data) < 100 :
        color='cell_bg_tres'
    else :
        color='cell_bg_nok'
    return color

@app.route('/')
def index():
    #variable
    data_hour={}
    data_min={}
    dtnow=GetToday()
    #data
    datahour=pd.read_csv(hourscp)
    datamin=pd.read_csv(minscp)
    colum_hour=datahour.columns
    colum_min=datamin.columns
    for ch in colum_hour:
        data_hour[ch]=datahour[ch].to_list()
    print(data_hour)
    for cm in colum_min:
        data_min[cm]=datamin[cm].to_list()
    print('data minute')
    print(data_min)
    #nowstr=ConvertDatetoStr(dtnow,format='%d-%m-%Y %H:%M')
    dtstr=ConvertDatetoStr(dtnow,format='%d-%m-%Y')
    nowstr=f"{dtstr} {data_min['CDR_HOUR'][-1]}"
    return render_template('monitor_scp.html',datahour=data_hour,datamin=data_min,now=nowstr)

@app.route('/sdp')
def sdp():
    #variable
    data_hour={}
    data_min={}
    dtnow=GetToday()
    #data
    datahour=pd.read_csv(hoursdp)
    datamin=pd.read_csv(minsdp)
    colum_hour=datahour.columns
    colum_min=datamin.columns
    for ch in colum_hour:
        data_hour[ch]=datahour[ch].to_list()
    print(data_hour['CDR_HOUR'])
    for cm in colum_min:
        data_min[cm]=datamin[cm].to_list()
    print(data_min['CDR_HOUR'])
    #nowstr=ConvertDatetoStr(dtnow,format='%d-%m-%Y %H:%M')
    dtstr=ConvertDatetoStr(dtnow,format='%d-%m-%Y')
    nowstr=f"{dtstr} {data_min['CDR_HOUR'][-1]}"
    return render_template('monitor_sdp.html',datahour=data_hour,datamin=data_min,now=nowstr)

@app.route('/errmon')
def errormon():
    #data
    data=pd.read_csv(errfl)
    for c in data.columns:
        if 'ERR' in c :
            colname=f'{c}_CLR'
            data[colname]=data[c].apply(bgError)
    data['CDR_HOUR']=data['CDR_HOUR'].apply(lambda x : f'0{x}' if int(x) < 10 else x)
    data_error=reversed(data.to_dict('records'))
    dtnow=GetToday()
    dtstr=ConvertDatetoStr(dtnow,format='%H:%M')
    nowstr=f"{data['CDR_DATE'][0]} {dtstr}"
    return render_template('monitor_error.html',data=data_error,now=nowstr)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8686')