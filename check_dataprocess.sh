#! /bin/bash

basedir='/home/scpsdpdev/dashboard_sdp'
cd $basedir

#check data minute
nminute=`ps -aef | grep GetNewDataMinute2 | grep -v grep | wc -l`
if [ $nminute -lt 1 ]
then
   nohup /home/scpsdpdev/dashboard/bin/python3.11 GetNewDataMinute2.py &
fi

#check data hour
nhour=`ps -aef | grep GetNewDataHour2 | grep -v grep | wc -l`
if [ $nhour -lt 1 ]
then
   nohup /home/scpsdpdev/dashboard/bin/python3.11 GetNewDataHour2.py &
fi