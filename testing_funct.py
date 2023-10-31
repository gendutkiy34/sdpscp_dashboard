import os
from modules.DataProcess import ScpData
from modules.general import GetToday

path=os.getcwd()

today=GetToday()
rawscp=f'{path}/rawdata/scp_data_raw.csv'
datascp=ScpData(pathfile=rawscp)
data1=datascp.VerifyData()
data2=data1[data1['DATE'] == today.date()]
print(data2.tail())

