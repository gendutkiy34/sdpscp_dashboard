import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017
import pandas as pd


app = Flask(__name__)
app.config["SECRET_KEY"] = 'tO$and!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()


@app.route('/')
def index():
    #variable
    rawscp='./rawdata/data_scp_today.csv'
    rawsdp='./rawdata/data_sdp_today.csv'
    list_col=[]
    list_sr=[]
    list_att=[]
    #data scp 
    datascp=ScpData(pathfile=rawscp)
    scpatt,scpsuc,scpsr=datascp.SumDataToday()
    list_sr.append(scpsr)
    list_att.append(str(scpatt))
    print(f'voice attemp : {scpatt} , voice success : {scpsuc} , voice success rate : {scpsr} %')
    list_scp_att,list_scp_min=datascp.HourMinScp()
    if scpsr >= 99 :
        list_col.append('#9aed13')
    elif scpsr > 98 and scpsr < 98:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    #data sdp
    datasdp=SdpData(pathfile=rawsdp)
    bmtatt,bmtsuc,bmtsr=datasdp.SumDataToday(accflag=67)
    list_sr.append(bmtsr)
    list_att.append(str(bmtatt))
    if bmtsr >= 99 :
        list_col.append('#9aed13')
    elif bmtsr > 98 and bmtsr < 99:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    print(f'bulkmt attemp : {bmtatt} , bulkmt success : {bmtsuc} , bulkmt success rate : {bmtsr} %')
    bmoatt,bmosuc,bmosr=datasdp.SumDataToday(accflag=66)
    list_sr.append(bmosr)
    list_att.append(str(bmoatt))
    if bmosr >= 99 :
        list_col.append('#9aed13')
    elif bmosr > 98 and bmosr < 99:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    print(f'bulkmo attemp : {bmoatt} , bulkmo success : {bmosuc} , bulkmo success rate : {bmosr} %')
    digatt,digsuc,digsr=datasdp.SumDataToday(accflag=68)
    list_sr.append(digsr)
    list_att.append(str(digatt))
    if digsr >= 99 :
        list_col.append('#9aed13')
    elif digsr > 98 and digsr < 99:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    print(f'digital attemp : {digatt} , digital success : {digsuc} , digital success rate : {digsr} %')
    smtatt,smtsuc,smtsr=datasdp.SumDataToday(accflag=73)
    list_sr.append(smtsr)
    list_att.append(str(smtatt))
    if smtsr >= 99 :
        list_col.append('#9aed13')
    elif smtsr > 98 and smtsr < 99:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    print(f'sbulkmt attemp : {smtatt} , sbulkmt success : {smtsuc} , sbulkmt success rate : {smtsr} %')
    smoatt,smosuc,smosr=datasdp.SumDataToday(accflag=72)
    list_sr.append(smosr)
    list_att.append(str(smoatt))
    if smosr >= 99 :
        list_col.append('#9aed13')
    elif smosr > 98 and smosr < 99:
        list_col.append('#edd713')
    else :
        list_col.append('#ed1372')
    print(f'sbulkmo attemp : {smoatt} , sbulkmo success : {smosuc} , sbulkmo success rate : {smosr} %')
    list_bmo,list_bmt,list_dig,list_smo,list_smt,list_sdp_min=datasdp.HourMinSdp()
    print(list_sr)
    print(list_col)
    print(list_att)
    return render_template('dashboardV2.html',scpatt=f"{scpatt:,d}",scpsr=scpsr,bmoatt=f"{bmoatt:,d}",bmosr=bmosr,bmtatt=f"{bmtatt:,d}",
                           bmtsr=bmtsr,digatt=f"{digatt:,d}",digsr=digsr,smoatt=f"{smoatt:,d}",smosr=smosr,smtatt=f"{smtatt:,d}",
                           smtsr=smtsr,listscpmin=list_scp_min,listscpatt=list_scp_att,listsdpmin=list_sdp_min,
                           listbmoatt=list_bmo,listbmtatt=list_bmt,listdigatt=list_dig,listsmoatt=list_smo,
                           listsmtatt=list_smt,list_col=list_col,list_sr=list_sr,lissumatt=list_att)


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


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8081')
    #app.run(debug=True,port='8081')