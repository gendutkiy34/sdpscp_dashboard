import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.DataProcess import ScpData,SdpData,ScpDataD017,SdpDataD017
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler


app = Flask(__name__)
app.config['SECRET_KEY'] = 'tO$&!|0wkamvVia0?n$NqIRVWOG'


upload_dir = 'input/'
ALLOWED_EXTENSIONS = {'csv'}
app.config['UPLOAD_FOLDER'] = upload_dir
app.app_context().push()


#variable
list_sk=[100,200,133,150,300,400]
listsr=[66,67,68,72,73]

rawd3scp='./rawdata/scp_data_d017.csv'

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    status=0
    dt=GetToday()
    dtstr=ConvertDatetoStr(dt,format='%Y%m%d%H%M%S')
    file=None
    print(status)
    if request.method == "POST":
        if request.files['filepath'] != '' :
            file=request.files['filepath']
            if file and allowed_file(file.filename):
                flname = f'{upload_dir}{dtstr}.csv'
                file.save(flname)
                status=1
    print(status)
    return render_template('test_upload.html',status=status )


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


@app.route("/test_chart")
def dynamic_chart():
    chart_script=chart().encode('utf8') 
    return render_template('test_dashboard_dynamic.html')

@app.route('/upload', methods = ['GET', 'POST'])
def upload():
    status=0
    dt=GetToday()
    dtstr=ConvertDatetoStr(dt,format='%Y%m%d%H%M%S')
    file=None
    print(status)
    if request.method == "POST":
        if request.files['filepath'] != '' :
            file=request.files['filepath']
            if file and allowed_file(file.filename):
                flname = f'{upload_dir}{dtstr}.csv'
                file.save(flname)
                status=1
    print(status)
    return render_template('test_upload.html',status=status )


def chart():
    chartscript="""new ApexCharts(document.querySelector("#columnChart"), {
                    series: [{
                      name: 'Net Profit',
                      data: [44, 55, 57, 56, 61, 58, 63, 60, 66]
                    }, {
                      name: 'Revenue',
                      data: [76, 85, 101, 98, 87, 105, 91, 114, 94]
                    }, {
                      name: 'Free Cash Flow',
                      data: [35, 41, 36, 26, 45, 48, 52, 53, 41]
                    }],
                    chart: {
                      type: 'bar',
                      height: 350
                    },
                    plotOptions: {
                      bar: {
                        horizontal: false,
                        columnWidth: '55%',
                        endingShape: 'rounded'
                      },
                    },
                    dataLabels: {
                      enabled: false
                    },
                    stroke: {
                      show: true,
                      width: 2,
                      colors: ['transparent']
                    },
                    xaxis: {
                      categories: ['Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct'],
                    },
                    yaxis: {
                      title: {
                        text: '$ (thousands)'
                      }
                    },
                    fill: {
                      opacity: 1
                    },
                    tooltip: {
                      y: {
                        formatter: function(val) {
                          return "$ " + val + " thousands"
                        }
                      }
                    }
                  }).render();"""
    return chartscript

if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')