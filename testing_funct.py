import os
import pandas as pd
import time
from datetime import datetime
from modules.connection import OracleCon
from modules.DataProcess import ScpData,SdpData


pathdir=os.getcwd()
rawscp=f'{pathdir}/rawdata/data_sdp_today.csv'


datascp=SdpData(rawscp)
datatoday=datascp.VerifyDataToday()
list_att,list_succ,list_hour=datascp.HourlyDataToday(accflag=68)
print(datatoday.head())
print(list_hour)
print(list_att)







