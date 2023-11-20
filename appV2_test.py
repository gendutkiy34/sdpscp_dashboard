import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from livereload import Server


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'

app.app_context().push()

#variable
list_sk=[100,200,133,150,300,400]
listsr=[66,67,68,72,73]

rawd3scp='./rawdata/scp_data_d017.csv'

@app.route('/')
def index():
    now=GetToday()
    nostr=ConvertDatetoStr(now,format='%Y-%m-%d %H:%M:%S')
    print(nostr)
    return render_template('dashboard_test.html',nwtime=nostr)


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
   
    return render_template('baseappV2.html')


@app.route("/joke")
def joke():
    joke = index()
    return joke

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')