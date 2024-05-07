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
rawsdp='./rawdata/sdp_raw_hour.csv'
realtm='./rawdata/data_realtime_minute.csv'
errrealtm='./rawdata/error_realtime_minute.csv'
rawdim='/home/scpsdpdev/alert/Scripts/output/internal_diameter_statistic.json'
smsque='/home/scpsdpdev/alert/Scripts/output/smsc_queue_statistic.json'
sdperrorcode='sdp_error_code.json'

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

@app.route('/sdptoday') 
def sdptoday() :
    dataraw=pd.read_csv(rawsdp)
    errcode=ReadJsonFile(sdperrorcode)
    raw_today=dataraw[dataraw['REMARK']=='day0']
    item={}

    #toperror
    raw_err=raw_today[~raw_today['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031])]
    df_sum=raw_err.groupby(['CDR_DATE','ACCESSFLAG','BASICCAUSE'])['TOTAL'].sum().reset_index()
    df_sum['RANK']=df_sum.groupby(['ACCESSFLAG'])['TOTAL'].rank(method='dense',ascending=False)
    df_top5=df_sum[df_sum['RANK'] <= 5]
    df_top5.sort_values(by=['ACCESSFLAG','TOTAL'],ascending=[False,False],inplace=True)
    df_top5['DESCRIPTION']=df_top5['BASICCAUSE'].apply(lambda x : errcode.get(str(x)))
    dict_top={}
    for ac in [66,67,68,72,73] :
        temp=df_top5[df_top5['ACCESSFLAG']==ac].sort_values(['TOTAL'],ascending=False)
        dict_top[ac]=temp.to_dict('records')
    item['toperror']=dict_top

    #traffic
    for acc in [66,67,68,72,73] :
        raw_temp=raw_today[raw_today['ACCESSFLAG']==acc]
        raw_succ=raw_today[(raw_today['ACCESSFLAG']==acc) & (raw_today['INTERNALCAUSE'].isin([2001,4010,4012,5030,5031]))]
        att=sum(raw_temp['TOTAL'].to_list())
        suc=sum(raw_succ['TOTAL'].to_list())
        dfrank=raw_temp[['CP_NAME','TOTAL']].groupby('CP_NAME')['TOTAL'].sum().reset_index()
        dfrank.sort_values(by='TOTAL', ascending=False,inplace=True)
        dfrank['RANK']=dfrank['TOTAL'].rank(ascending=False)
        dfrank=dfrank[dfrank['RANK'] <= 5]
        dftemp=dfrank[['CP_NAME','TOTAL']]
        dftemp['TOTAL']=dftemp['TOTAL'].apply(lambda x : "{:,}".format(x))
        item[f'top{acc}']=dftemp.to_dict('records')
        item[f'att{acc}']=f'{att:,}' 
        item[f'suc{acc}']=f'{suc:,}' 
        item[f'sucpercent{acc}']=round((suc/att)*100,2)

    #top basiccause
    error_raw=raw_today[(raw_today['BASICCAUSE'].isin([601,83]) ) & (raw_today['REMARK']=='day0')]
    for bc in [83,601] :
      raw_temp=error_raw[error_raw['BASICCAUSE']==bc]
      dfrank=raw_temp[['CP_NAME','TOTAL']].groupby('CP_NAME')['TOTAL'].sum().reset_index()
      dfrank.sort_values(by='TOTAL', ascending=False,inplace=True)
      dfrank['RANK']=dfrank['TOTAL'].rank(ascending=False)
      dfrank=dfrank[dfrank['RANK'] <= 5]
      dftemp=dfrank[['CP_NAME','TOTAL']]
      dftemp['TOTAL']=dftemp['TOTAL'].apply(lambda x : "{:,}".format(x))
      item[f'top{bc}']=dftemp.to_dict('records')
    raw_ocs=raw_today[(raw_today['BASICCAUSE']==940) & (raw_today['INTERNALCAUSE'].isna())]
    dfrank=raw_ocs[['CP_NAME','TOTAL']].groupby('CP_NAME')['TOTAL'].sum().reset_index()
    dfrank.sort_values(by='TOTAL', ascending=False,inplace=True)
    dfrank['RANK']=dfrank['TOTAL'].rank(ascending=False)
    dfrank=dfrank[dfrank['RANK'] <= 5]
    dftemp=dfrank[['CP_NAME','TOTAL']]
    dftemp['TOTAL']=dftemp['TOTAL'].apply(lambda x : "{:,}".format(x))
    item[f'topocs']=dftemp.to_dict('records')
    dtnow=GetToday()
    dtstr=ConvertDatetoStr(dtnow,format='%H:%M')
    nowstr=f"{raw_today['CDR_DATE'][0]} {dtstr}"
    return render_template('monitor_sdp_today.html',data=item,now=nowstr)


@app.route('/trafficall') 
def TrafficAttempt() :
    dataraw=pd.read_csv(realtm)
    for c in ['MM', 'PK', 'BULK_MO', 'BULK_MT', 'DIGITAL_SERVICE','SUBSCRIPTIONBULK_MO', 'SUBSCRIPTIONBULK_MT'] :
        dataraw[c]=dataraw[c].astype('int')
        newcol=f'{c}_colour'
        dataraw[newcol]=dataraw[c].apply(lambda x : "cell_bg_ok" if int(x) >= 100 else ("cell_bg_tres" if int(x) < 100 and int(x) > 0 else "cell_bg_nok"))
    datadict=dataraw.to_dict('records')
    nowstr=f"{datadict[0]['CDR_DATE']} {datadict[0]['HOURMINUTE']}"
    return render_template('monitor_traffic.html',data=datadict,now=nowstr)

@app.route('/errminute') 
def ErrorMinute() :
    dataraw=pd.read_csv(errrealtm)
    for c in ['err83', 'err601', 'billing_timeout','err5000', 'err5004', 'err5005', 'err5012', 'err6000'] :
        dataraw[c]=dataraw[c].astype('int')
        newcol=f'{c}_colour'
        dataraw[newcol]=dataraw[c].apply(lambda x : "cell_bg_ok" if int(x) < 1 else "cell_bg_nok")
    datadict=dataraw.to_dict('records')
    nowstr=f"{datadict[0]['CDR_DATE']} {datadict[0]['HOURMINUTE']}"
    return render_template('monitor_errminute.html',data=datadict,now=nowstr)

@app.route('/errnode') 
def ErrorNode() :
    dimjson=ReadJsonFile(pathfile=rawdim)
    smscjson=ReadJsonFile(pathfile=smsque)
    for k,v in dimjson.items():
      if int(v['error_count']) < 1 :
        v['cell_colour']='cell_bg_ok'
      elif int(v['error_count']) >= 1 :
        v['cell_colour']='cell_bg_nok'
      if int(v['connection_error']) < 1 :
        v['con_cell_colour']='cell_bg_ok'   
      elif int(v['connection_error']) >= 1 :
        v['con_cell_colour']='cell_bg_nok'
    nowstr=dimjson['jktmmpsdpprov01']['timestamp']
    for k,v in smscjson.items():
      for i in range (1,7) :
            if int(v[f'smsapp{i}']) < 1 :
                v[f'smsapp{i}_colour']='cell_bg_ok'
            elif int(v[f'smsapp{i}']) >= 1 and int(v[f'smsapp{i}']) <= 1000 :
                v[f'smsapp{i}_colour']='cell_bg_tres'    
            elif int(v[f'smsapp{i}']) >= 1001:
                v[f'smsapp{i}_colour']='cell_bg_nok'
    os.system(f'echo {smscjson}')
    return render_template('monitor_errnode.html',data=dimjson,now=nowstr,data2=smscjson)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8686')