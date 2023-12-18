import schedule
import os
from GetNewDataMinute2 import ScpMinute,JoinDataScp

schedule.every(3).minutes.do(ScpMinute)
schedule.every(4).minutes.do(JoinDataScp)

while True:
    schedule.run_pending()
    time.sleep(1)