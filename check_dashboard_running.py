import os
from datetime import datetime

temp=os.popen('ps -aef | grep appmonitor.py | grep -v grep | wc -l')
n=temp.read().replace('\n','')
today=datetime.now()
dt_string=today.strftime("%Y-%m-%d %H:%M:%S")
if int(n) < 1 :
    os.chdir('/home/scpsdpdev/dashboard_sdp')
    os.system('echo  "{} - dashboard off"'.format(dt_string))
    os.system('nohup /home/scpsdpdev/dashboard/bin/python3.11 appmonitor.py &')
else :
    os.system('echo  "{} - dashboard running..."'.format(dt_string))
