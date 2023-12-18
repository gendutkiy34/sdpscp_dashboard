# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 20:07:23 2023

@author: HS00935501
"""

import os


homdir='/home/scpsdpdev'
dasbdir='/home/scpsdpdev/dashboard_sdp'

#check data raw
cmd="ps -aef | grep -i 'UpdateDataToday' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 UpdateDataToday.py &')
print(f'UpdateDataToday status {n}')
    
#check data rawD017
cmd="ps -aef | grep -i 'GetDataD017' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 GetDataD017.py &')
print(f'GetDataD017 status {n}')
    
    
#check joincsv
cmd="ps -aef | grep -i 'JoinCsvFile' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 JoinCsvFile.py &')
print(f'JoinCsvFile status {n}')

    
#check dashboard
cmd="ps -aef | grep -i 'appV2' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 appV2.py &')
print(f'appV2 status {n}')   


#check  minute new
cmd="ps -aef | grep -i 'GetNewDataMinute2' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 GetNewDataMinute2.py &')
print(f'GetNewDataMinute2 status {n}')   

#check  hour new
cmd="ps -aef | grep -i 'GetNewDataHour2' | grep -v grep | wc -l"
n=os.popen(cmd).read().replace('\n','')
if n == '0' :
    os.chdir(homdir)
    os.popen('source dashboard/bin/activate')
    os.chdir(dasbdir)
    os.popen('nohup /home/scpsdpdev/dashboard/bin/python3.11 GetNewDataHour2.py &')
print(f'GetNewDataHour2 status {n}')   
