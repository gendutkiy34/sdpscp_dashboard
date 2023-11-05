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
    return render_template('baseappV2.html')

@app.route('/scpd017')
def scpd017():
    return render_template('baseappV2.html')

@app.route('/testvar')
def testvar():
    list_test=[11,33,55,88,99,10]
    dict_test={'name':'akulaku','nominal':123455,'date':'12-12-2012'}
    dictest2={'env':'scp','data':[21,34,56,78,300,287]}
    return render_template('dashboard_teslayout2.html',dic=dict_test,lis=list_test,dic2=dictest2)

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='8081')
    #app.run(debug=True,port='8081')