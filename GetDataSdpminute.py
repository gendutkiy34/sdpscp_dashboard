import schedule
import os
from GetNewDataMinute2 import SdpMinute,JoinDataSdp

schedule.every(3).minutes.do(SdpMinute)
schedule.every(4).minutes.do(JoinDataSdp)

while True:
    schedule.run_pending()
    time.sleep(1)