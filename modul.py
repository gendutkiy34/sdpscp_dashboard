import os
from formapp import FormHttpReq,FormLog
from modules.htttprequest import ReqHttp


def SimulHttp():
    flag=False
    SimForm=FormHttpReq()
    tempmsg=""
    errortext=""
    errorcode=""
    if SimForm.validate_on_submit():
        flag=True
        tempmsg="""http://{0}/push?TYPE=0&MESSAGE={1}&MOBILENO={2}&ORIGIN_ADDR={3}&REG_DELIVERY=1&PASSWORD={4}&USERNAME={5}""".format(
            SimForm.sdpenv.data,SimForm.msg.data,SimForm.msisdn.data,SimForm.sdc.data,SimForm.passw.data,SimForm.username.data)
        SimForm.msg.data=''
        SimForm.passw.data=''
        SimForm.sdc.data=''
        SimForm.sdpenv.data=''
        SimForm.username.data=''
        SimForm.dt.data=''
        SimForm.msisdn.data=''
        errorcode,errortext=ReqHttp(tempmsg)
    return SimForm,errorcode,errortext,flag


