import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017
import pandas as pd


app = Flask(__name__)
app.config["SECRET_KEY"] = 'tO$and!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()

#variable
listaccflag=[66,67,68,72,73]

@app.route('/')
def index():
    #variable
    data_scp={}
    data_sdp={}
    minscp='./rawdata/data_scp_today.csv'
    minsdp='./rawdata/data_sdp_today.csv'
    d3scp='./rawdata/scp_data_d017.csv'
    d3sdp='./rawdata/sdp_data_d017.csv'
    #data scp 
    datascp=ScpDataD017(pathfile=d3scp)
    dataminscp=ScpData(pathfile=minscp)
    scpatt,scpsuc,scpsr=datascp.SumDataToday()
    list_scp_att,list_scp_min=dataminscp.HourMinScp()
    data_scp['att']=f'{scpatt:,}'
    data_scp['suc']=scpsuc
    data_scp['sr']=scpsr
    data_scp['attmin']=list_scp_att
    data_scp['listmin']=list_scp_min
    print(f"voice attemp : {data_scp['att']} , voice success : {data_scp['suc']} , voice success rate : {data_scp['sr']} %")
    if data_scp['sr'] >= 99 :
        data_scp['colscp']='#9aed13'
    elif data_scp['sr'] > 98 and data_scp['sr'] < 98:
        data_scp['colscp']='#edd713'
    else :
        data_scp['colscp']='#ed1372'
    print(f'data scp : {data_scp}')
    #data sdp
    datasdp=SdpDataD017(pathfile=d3sdp)
    for af in listaccflag:
        sumatt=f'att{af}'
        sumsuc=f'suc{af}'
        sumsr=f'sr{af}'
        att,suc,sr=datasdp.SumAccToday(accflag=af)
        data_sdp[sumatt]=f'{att:,}'
        data_sdp[sumsuc]=suc
        data_sdp[sumsr]=sr
    if data_sdp['sr66'] >= 99 :
        data_sdp['col66']='#9aed13'
    elif data_sdp['sr66'] > 98 and data_sdp['sr66'] < 99:
        data_sdp['col66']='#edd713'
    else :
        data_sdp['col66']='#ed1372'
    print(f"bulkmo attemp : {data_sdp['att66']} , bulkmo success : {data_sdp['suc66']} , bulkmo success rate : {data_sdp['sr66']} %")
    if data_sdp['sr67'] >= 99 :
        data_sdp['col67']='#9aed13'
    elif data_sdp['sr67'] > 98 and data_sdp['sr67'] < 99:
        data_sdp['col67']='#edd713'
    else :
        data_sdp['col67']='#ed1372'
    print(f"bulkmt attemp : {data_sdp['att67']} , bulkmt success : {data_sdp['suc67']} , bulkmt success rate : {data_sdp['sr67']} %")
    if data_sdp['sr68'] >= 99 :
        data_sdp['col68']='#9aed13'
    elif data_sdp['sr68'] > 98 and data_sdp['sr68'] < 99:
        data_sdp['col68']='#edd713'
    else :
        data_sdp['col68']='#ed1372'
    print(f"digital attemp : {data_sdp['att68']} , digital success : {data_sdp['suc68']} , digital success rate : {data_sdp['sr68']} %")
    if data_sdp['sr72'] >= 99 :
        data_sdp['col72']='#9aed13'
    elif data_sdp['sr72'] > 98 and data_sdp['sr72'] < 99:
        data_sdp['col72']='#edd713'
    else :
        data_sdp['col72']='#ed1372'
    print(f"sbulkmt attemp : {data_sdp['att72']} , sbulkmt success : {data_sdp['suc72']} , sbulkmt success rate : {data_sdp['sr72']} %")
    if data_sdp['sr73'] >= 99 :
        data_sdp['col73']='#9aed13'
    elif data_sdp['sr73'] > 98 and data_sdp['sr73'] < 99:
        data_sdp['col73']='#edd713'
    else :
        data_sdp['col73']='#ed1372'
    print(f"sbulkmo attemp : {data_sdp['att73']} , sbulkmo success : {data_sdp['suc73']} , sbulkmo success rate : {data_sdp['sr73']} %")
    dataminsdp=SdpData(pathfile=minsdp)
    list_bmo,list_bmt,list_dig,list_smo,list_smt,list_sdp_min=dataminsdp.HourMinSdp()
    data_sdp['min66']=list_bmo
    data_sdp['min67']=list_bmt
    data_sdp['min68']=list_dig
    data_sdp['min72']=list_smo
    data_sdp['min73']=list_smt
    data_sdp['listmin']=list_sdp_min
    print(f'data sdp : {data_sdp}')
    return render_template('dashboardV2.html',dict_scp=data_scp,dict_sdp=data_sdp)


@app.route('/scpd017')
def scpd017():
    pathdir=os.getcwd()
    rawscp=f'{pathdir}/rawdata/scp_data_d017.csv'
    datascp=ScpDataD017(rawscp)
    att0,att1,att7,atth=datascp.Att()
    suc0,suc1,suc7,such=datascp.AttDia(diameter=2001)
    sk100,sk101,sk107,sk10h=datascp.AttRoam(diameter=2001,servicekey=100,roaming=1)
    rm0,rm1,rm7,rmh=datascp.AttRoam(roaming=1)
    return render_template('dashboard_scpd017.html',att0=att0,att1=att1,att7=att7,
                           listh=atth,suc0=suc0,suc1=suc1,suc7=suc7,sk100=sk100,sk101=sk101,
                           sk107=sk107,rm0=rm0,rm1=rm1,rm7=rm7)


@app.route('/sdptoday')
def sdptoday():
    topcp={}
    pathdir=os.path.abspath(os.path.dirname(__file__))
    rawsdp=f'{pathdir}/rawdata/sdp_data_d017.csv'
    datasdp=SdpDataD017(rawsdp)
    listsr=[66,67,68,72,73]
    dic_data={}
    for s in listsr :
        att_label=f'att{s}'
        suc_label=f'suc{s}'
        sr_label=f'sr{s}'
        att,succ,hour=datasdp.AttHourToday(accflag=s)
        sr,srhour=datasdp.SrHourToday(accflag=s)
        dic_data[att_label]=att
        dic_data[suc_label]=succ    
        dic_data[sr_label]=sr 
    list_hour=[]
    for h in hour :
        if len(str(h)) < 2:
            list_hour.append(f'0{h}')
        else :
            list_hour.append(h)
    dic_data['hour']=list_hour
    cprev,rev=datasdp.RevTop5()
    topcp['cprev']=cprev
    topcp['revenue']=rev
    cpatt,att=datasdp.AttTop5()
    topcp['cpatt']=cpatt
    topcp['attempt']=att
    dfsum=datasdp.SummaryToday()
    dic_data['summary']=dfsum
    print(dic_data)
    return render_template('dashboard_sdp_today.html',dic_sdp=dic_data,dic_top=topcp)


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8081')
    #app.run(debug=True,port='8081')