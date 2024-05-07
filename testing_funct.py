import pandas as pd
import os
import json
import time
from datetime import datetime,timedelta
from modules.general import GetToday
from Grep_data_daily_statistic import JoinDataOldNew


pd.options.mode.chained_assignment = None

import pandas as pd
import time
import os
import schedule
from datetime import datetime, timedelta
from modules.general import GetToday
from modules.connection import OracleCon
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,Sum2list
from modules.remoteclient import SshNode,GetRemoteFile
from GrepDataNew import GetDataMinute

#pathdir=os.getcwd()
#creden=f'{pathdir}/connections/mgwsit.json'
#data=SshNode(pathcred=creden,comm='hostname')
#print(data)
#GetRemoteFile(pathcred=creden,remotedir='/home/sdpuser/TechM_Sdp/output/scp_newdata_minute.csv',localdir='/home/scpsdpdev/dashboard_sdp/rawdata/scp_newdata_minute.csv')
JoinDataOldNew(env='scp')
