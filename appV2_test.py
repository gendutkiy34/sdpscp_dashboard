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
scprtm='./rawdata/scp_data_realtime.csv'
sdprtm='./rawdata/sdp_data_realtime.csv'
sdptop5='./rawdata/sdp_raw_top5cp.csv'
rawsdp='./rawdata/sdp_raw_hour.csv'

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

@app.route('/realtime')
def realtime() :
    #data
    datascp=pd.read_csv(scprtm)
    print(datascp.info())
    datascp['BFT_MM']=datascp['BFT_MM'].astype(int)
    datascp['BFT_PK']=datascp['BFT_PK'].astype(int)
    datasdp=pd.read_csv(sdprtm)
    dict_scp=datascp.to_dict('records')[-12:]
    dict_sdp=datasdp.to_dict('records')[-12:]
    print(dict_sdp)
    return render_template('realtime_sdpscp.html',datascp=dict_scp[::-1],datasdp=dict_sdp[::-1])


@app.route('/top5')
def top5() :
    dataraw=pd.read_csv(sdptop5)
    list_acc=[66,67,68,72,73]
    list_day=[0,1,7]
    dict_pd={}
    for ac in list_acc:
      item={}
      itemday={}
      dftemp=dataraw[dataraw['ACCESSFLAG']==ac]
      dftemp['RANK'] =dftemp.groupby(["REMARK"])["TOTAL"].rank(method="dense", ascending=False)
      top5=dftemp[dftemp['RANK'] <= 5]
      for d in list_day:
        subitem={}
        temp=top5[top5['REMARK']==f'day{d}'].sort_values(by=['TOTAL'],ascending=False)
        subitem['list_cp']=temp['CP_NAME'].values.tolist()
        subitem['list_total']=temp['TOTAL'].values.tolist()
        itemday[f'day{d}']=subitem
      dict_pd[ac]=itemday
    return render_template('top5.html',data=dict_pd)

@app.route('/sdptoday') 
def sdptoday() :
    dataraw=pd.read_csv(rawsdp)
    raw_today=dataraw[dataraw['REMARK']=='day0']
    item={}

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
    print(raw_today['CDR_DATE'][0])
    return render_template('sdp_today_monitor.html',data=item)
    
if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port='3034')
    #app.run(debug=True,port='3034')