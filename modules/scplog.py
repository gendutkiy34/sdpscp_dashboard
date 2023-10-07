import os
import json
from datetime import datetime
import paramiko
from modules.general import ConvertStrtoDate,ConvertDatetoStr,GetToday,SshNode

list_node=[
    {"nodename":"jktpkpscplog01",
     "host":"10.64.88.126",
     "username":"scpuser",
     "password":"0neT1meP@55"},
    {"nodename":"jktmmpscplog01",
     "host":"10.64.65.197",
     "username":"scpuser",
     "password":"0neT1meP@55"}
]

not_use=['CIEvent','StartAction','ConnectAction','DelimiterAction','onNotifyEvent',
           'COAction','IFAction','WaitForEventsAction','MultiAction','EventAction','Function',
         'ValidationAction','CUGAction','BFTAction','FoCAction','NumbTranslationAction','CDRAction','decodeAndSendSS7Msg','sendSS7Msg']

use_with_flag=['getChargingOverruleInfo','getFoCInfo','getNumbTranslationInfo',
               'getCUGInfo','getBFTInfo','NormalizationAction']


def GetScpLog(tgl=None,trxid=None):
    dt1=GetToday()
    dt2=ConvertStrtoDate(str(tgl),'%Y-%m-%d')
    print('date : ',tgl,', trxid : ',trxid)
    list_temp=[]
    if dt1.date() == dt2.date() :
        dt_string1=ConvertDatetoStr(tgl=dt2,format='%Y%m%d')
        for nd in list_node :
            cmd='cat /opt/logs/scp_app/*{0}* | grep -ah {1}'.format(dt_string1,trxid)
            stdout,sterr=SshNode(host=nd['host'],user=nd['username'],pwd=nd['password'],cmd=cmd)
            print('execute in node : ,', nd['nodename'],'command1 : ',cmd)
            for t in stdout:
                list_temp.append(t)
            print('result : ', len(list_temp))
            if len(list_temp) < 1 :
                dt_string2=ConvertDatetoStr(tgl=dt2,format='%Y-%m-%d')
                cmd2='zcat /opt/logs/scp_app/*{0}* | grep -ah {1}'.format(dt_string2,trxid)
                print('execute in node : ,', nd['nodename'],'command2 : ',cmd2)
                stdout,sterr=SshNode(host=nd['host'],user=nd['username'],pwd=nd['password'],cmd=cmd2)
                for t in stdout:
                    list_temp.append(t)
                print('result2 : ', len(list_temp))
                if len(list_temp) > 0 :
                    break
            else :
                break
    else :
        flag=0
        dt_string=ConvertDatetoStr(dt2,'%Y-%m-%d')
        for nd in list_node :
            cmd='zcat /opt/logs/scp_app/*{0}* | grep -ah {1}'.format(dt_string,trxid)
            stdout,sterr=SshNode(host=nd['host'],user=nd['username'],pwd=nd['password'],cmd=cmd)
            print('execute in node : ,', nd['nodename'],'command1 : ',cmd)
            for t in stdout:
                list_temp.append(t)
            if len(list_temp) > 0 :
                    break
    return list_temp


def ExPusNot(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage' :
      if 'RequestReceived' in t[1] :
        temp_t=t[1].split()
        temp_dict['msisdn']=temp_t[1].split(':')[1]
        temp_dict['msgtype']=temp_t[6].split(':')[1]
      elif 'userdata' in t[1] :
        temp_t=t[1].split(":")
        temp_dict['userdata']=temp_t[1]
      elif 'ServiceAddress' in t[1] and 'UserData' in t[1] :
        temp_t=t[1].split(",")
        temp_dict['msisdn']=temp_t[0].split(':')[1]
        temp_dict['UserData']=temp_t[1].split(':')[1]
      else :
        temp_dict[t[0]]=t[1]
    elif t[0] == 'userdata' :
      temp_dict[t[0]]=t[1]
  try :
    if 'msisdn' in temp_dict and 'msgtype' in temp_dict :
      msg="""{0} --> {1} -- {2} : EventTID={3} , msisdn={4} , msgtype={5}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('msisdn'),
                                       temp_dict.get('msgtype'))
    elif 'msisdn' in temp_dict and 'UserData' in temp_dict :
      msg="""{0} --> {1} -- {2} : EventTID={3} , msisdn={4} , UserData={5}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('msisdn'),
                                       temp_dict.get('UserData'))
    elif 'userdata' in temp_dict :
      msg="""{0} --> {1} -- {2} : EventTID={3} , msisdn={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('userdata'))
  except Exception :
    msg="""{0} --> {1} -- {2} : EventTID={3}--failed extract data""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'))
  return temp_dict.get('TimeStamp'),msg


def IdpProc(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      if 'ServiceId' in t[1] and 'CGPA' in t[1] :
        temp_t=t[1].split()
        for t2 in temp_t:
          temp_t2=t2.split(':')
          temp_dict[temp_t2[0]]=temp_t2[1]
  if 'ServiceId' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , CGPA={4} , CDPA={5} , CDPA-BCD={6} , RedirectingPartyId={7} , ServiceId={8} , VLR={9} , CallType={10}"""\
    .format(temp_dict.get('TimeStamp'),
            temp_dict.get('EventCategory'),
            temp_dict.get('EventType'),
            temp_dict.get('EventTID'),
            temp_dict.get('CGPA'),
            temp_dict.get('CDPA'),
            temp_dict.get('CDPA-BCD'),
            temp_dict.get('RedirectingPartyId'),
            temp_dict.get('ServiceId'),
            temp_dict.get('VLR'),
            temp_dict.get('CallType'))
    return temp_dict.get('TimeStamp'),msg


def ExtractCCx(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      if 'SessionId' in t[1] :
        temp_t=t[1].split(',')
        ses=temp_t[0].split(':')
        temp_dict[ses[0].replace(" ","")]=ses[1].split()[0]
        temp_dict[ses[1].split()[1].replace(" ","")]=ses[2]
        temp_dict[temp_t[1].split(':')[0].replace(" ","")]=temp_t[1].split(':')[1]
      else :
        temp_t=t[1].split(":")
        temp_dict[temp_t[0].replace(" ", "")]=temp_t[1]
  if 'GrantedCCTime' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , Granted CCTime={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('GrantedCCTime'))
  elif 'CCA-IResultCode' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , CCA-I ResultCode={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CCA-IResultCode'))
  elif 'CCA-TResultCode' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , CCA-T ResultCode={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CCA-TResultCode'))
  elif 'CCR-TUsedCCTime' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , CCA-T UsedCCTime={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CCR-TUsedCCTime'))
  elif 'AltSessionId' in temp_dict:
    msg="""{0} --> {1} -- {2} : EventTID={3} , SessionId={4} , AltSessionId={5} , VLR={6}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('SessionId'),
                                       temp_dict.get('AltSessionId'),
                                       temp_dict.get('VLR'))
  return temp_dict.get('TimeStamp'),msg


def NormAct(data):
  temp_dict={}
  flag=''
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage' :
      if 'InputNumber' in t[1] and 'NormalizedNumber' in t[1]:
        temp_t=t[1].split(",")
        temp_dict['InputNumber']=temp_t[0].split(':')[1]
        temp_dict['NormalizedNumber']=temp_t[1].split(':')[1]
        temp_dict['RuleName']=temp_t[2].split(':')[1]
        temp_dict['OutNai']=temp_t[2].split(':')[1]
  if temp_dict.get('EventType') ==  'ResponeData':
    msg="""{0} --> {1} -- {2} : EventTID={3} , InputNumber={4} , NormalizedNumber={5} , RuleName={6} , OutNai={7} """.format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('InputNumber'),
                                       temp_dict.get('NormalizedNumber'),
                                       temp_dict.get('RuleName'),
                                       temp_dict.get('OutNai'))
    return temp_dict.get('TimeStamp'),msg
  else:
    return None


def GetNumTras(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage' :
      if 'TMDN' in t[1] and 'PName' in t[1] :
        if ',' in t[1] :
          temp_t=t[1].split(",")
        else :
          temp_t=t[1].split()
        for t2 in temp_t:
           t3=t2.split(":")
           temp_dict[t3[0].replace(" ","")]=t3[1]
  try :
    if temp_dict.get('EventType') != 'RequestData' :
      msg="""{0} --> {1} -- {2} : EventTID={3} , msisdn={4} , tmdn={5} , outna={6} , pname={7}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('MSISDN'),
                                       temp_dict.get('TMDN'),
                                       temp_dict.get('OutNOA'),
                                       temp_dict.get('PName'))
      return temp_dict.get('TimeStamp'),msg
  except Exception as e:
    return None


def GetFoc(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      try :
        temp_t=t[1].split(",")
        temp_dict['CGPA']=temp_t[2].split(':')[1]
        temp_dict['CDPA']=temp_t[3].split(':')[1]
        temp_dict['VLR']=temp_t[4].split(':')[1]
        temp_dict['CallType']=temp_t[3].split(':')[1]
        temp_dict['ErrorCode']=temp_t[5].split(':')[1]
        temp_dict['ErrorMessage']=temp_t[1].split(':')[1]
      except Exception :
        temp_t=t[1].split(",")
        temp_dict['CGPA']=temp_t[0].split(':')[1]
        temp_dict['CDPA']=temp_t[1].split(':')[1]
        temp_dict['VLR']=temp_t[2].split(':')[1]
        temp_dict['CallType']=temp_t[3].split(':')[1]
  try :
    if temp_dict.get('EventType') != 'RequestData' :
      msg="""{0} --> {1} -- {2} : EventTID={3} , CGPA={4} , CDPA={5} , VLR={6} , CallType={7}, ErrorCode={8} , ErrorMessage={9} """.format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CGPA'),
                                       temp_dict.get('CDPA'),
                                       temp_dict.get('VLR'),
                                       temp_dict.get('CallType'),
                                       temp_dict.get('ErrorCode'),
                                       temp_dict.get('ErrorMessage'))
      return temp_dict.get('TimeStamp'),msg
  except Exception :
    return None


def GetCug(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      if 'ServiceAddress' in t[1]:
        pass
      else :
        temp_t=t[1].split(",")
        for t2 in temp_t:
          t3 = t2.split(":")
          temp_dict[t3[0].replace(" ","")]=t3[1]
        msg="""{0} --> {1} -- {2} : EventTID={3} , CGPA={4} , CDPA={5} , ErrorCode={6} , ErrorMessage={7} """.format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CGPA'),
                                       temp_dict.get('CDPA'),
                                       temp_dict.get('ErrorCode'),
                                       temp_dict.get('ErrorMessage'))
        return temp_dict.get('TimeStamp'),msg


def ExtractUni(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      temp_dict[t[0]]=t[1]
  msg="""{0} --> {1} -- {2} : EventTID={3} , EventMessage={4}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('EventMessage'))
  return temp_dict.get('TimeStamp'),msg


def ChOvRl(data):
  temp_dict={}
  if data.get('EventType') == 'RequestData':
    pass
  else :
    for t in data.items():
      if t[0] == 'TimeStamp' :
        temp_dict[t[0]]=t[1]
      elif t[0] == 'EventType' :
        temp_dict[t[0]]=t[1]
      elif t[0] == 'EventCategory' :
        temp_dict[t[0]]=t[1]
      elif t[0] == 'EventTID' :
        temp_dict[t[0]]=t[1]
      elif t[0] == 'EventMessage':
        temp_t=t[1].split(",")
        temp_dict['OverruleStatus']=temp_t[1].split(':')[1]
        temp_dict['ReturnCode']=temp_t[2].split(':')[1]
    msg="""{0} --> {1} -- {2} : EventTID={3} , OverruleStatus={4} , ReturnCode={5}""".format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('OverruleStatus'),
                                       temp_dict.get('ReturnCode'))
    return temp_dict.get('TimeStamp'),msg

def GetBft(data):
  temp_dict={}
  for t in data.items():
    if t[0] == 'TimeStamp' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventType' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventCategory' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventTID' :
      temp_dict[t[0]]=t[1]
    elif t[0] == 'EventMessage':
      if 'ServiceAddress' in t[1]:
        pass
      else :
        temp_t=t[1].split(",")
        for t2 in temp_t:
          t3=t2.split(':')
          temp_dict[t3[0].replace(" ","")]=t3[1]
        msg="""{0} --> {1} -- {2} : EventTID={3} , CGPA={4} , CDPA={5} , VLR={6} , ServiceKey={8} , GrantedSeconds={9} , ResultCode={10} , SubscriberType={11} , CallType={12} , Region={13} , Service={14} , TPO={15} """.format(temp_dict.get('TimeStamp'),
                                       temp_dict.get('EventCategory'),
                                       temp_dict.get('EventType'),
                                       temp_dict.get('EventTID'),
                                       temp_dict.get('CGPA'),
                                       temp_dict.get('CDPA'),
                                       temp_dict.get('CGPA'),
                                       temp_dict.get('VLR'),
                                       temp_dict.get('ServiceKey'),
                                       temp_dict.get('GrantedSeconds'),
                                       temp_dict.get('ResultCode'),
                                       temp_dict.get('SubscriberType'),
                                       temp_dict.get('CallType'),
                                       temp_dict.get('Region'),
                                       temp_dict.get('Service'),
                                       temp_dict.get('TPO'))
        return temp_dict.get('TimeStamp'),msg


def SortFix(list_time,list_info):
  result=[]
  list_new=list(map(lambda x : datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"),list_time))
  result=[info for tm in sorted(list_time) for info in list_info if str(tm) in info]
  return result


def ExtractScpLog(list_log=None):
  list_time=[]
  list_info=[]
  list_result=[]
  for tx in list_log:
    data1=None
    temp=json.loads(tx)
    data_temp=temp.get('logData').get('data')
    if data_temp.get('EventCategory') == 'pushCallNotification':
      data1=ExPusNot(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'getChargingOverruleInfo' :
      data1=ChOvRl(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'getCUGInfo' :
      data1=GetCug(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'getFoCInfo':
      data1=GetFoc(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'getBFTInfo':
      data1=GetBft(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'getNumbTranslationInfo':
      data1=GetNumTras(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'NormalizationAction':
      data1=NormAct(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'onCCAInitial' or data_temp.get('EventCategory') =='onCCATerminate'\
    or data_temp.get('EventCategory') =='sendCCRInitial' or data_temp.get('EventCategory') =='sendCCRTerminate' :
      data1=ExtractCCx(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') == 'processIDP':
      data1=IdpProc(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    elif data_temp.get('EventCategory') not in not_use:
      data1=ExtractUni(data_temp)
      if data1 is not None:
        list_time.append(data1[0])
        list_info.append(data1[1])
    else :
      pass
  result=SortFix(list_time,list_info)
  for x in result:
    list_result.append(x)
  return list_result