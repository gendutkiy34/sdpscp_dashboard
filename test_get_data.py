import os
from flask import Flask,render_template,url_for,redirect,request
from modules.general import ReadJsonFile,ReadTxtFile,ConvertListToDict,GetToday,ConvertDatetoStr,ConvertStrtoDate
from modules.DbQuery import ReadConfig,ReadTrx,GetDataToday,GetListSuccessRate,GetPandasToday,PdtoCsv
import time
from datetime import timedelta
#from modules.connection import OracleCon
#from modules.GetData import GetDataNow,GetDataNew,GetData3DCur
#from modules.DataProcess import ScpData,SdpData
import pandas as pd



scp_colum=['CDR_HOUR', 'ATT_0', 'ATT_1', 'ATT_7', 'SUC_0', 'SUC_1',
       'SUC_7', 'ROAMATT_0', 'ROAMATT_1', 'ROAMATT_7', 'ROAMSUC_0', 'ROAMSUC',
       'ROAMSUC_7', 'NONROATT_0', 'NONROATT_1', 'NONROATT_7', 'NONROSUC_0',
       'NONROSUC_1', 'NONROSUC_7', 'MOCATT_0', 'MOCATT_1', 'MOCATT_7',
       'MOCSUC_0', 'MOCSUC_1', 'MOCSUC_7', 'MTCATT_0', 'MTCATT_1', 'MTCATT_7',
       'MTCSUC_0', 'MTCSUC_1', 'MTCSUC_7', 'MFCATT_0', 'MFCATT_1', 'MFCATT_7',
       'MFCSUC_0', 'MFCSUC_1', 'MFCSUC_7']

raw=[]
dfold=pd.DataFrame(raw,columns=scp_colum)
n=len(dfold.index)
print(type(n),n)