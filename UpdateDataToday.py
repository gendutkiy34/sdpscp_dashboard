import pandas as pd
import time
from modules.GetData import GetDataNew


while True :
    GetDataNew(env='scp')
    GetDataNew(env='sdp')
    time.sleep(300)