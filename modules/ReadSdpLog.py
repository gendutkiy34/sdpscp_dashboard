import json

def ExtractBulkMTAdaptor(data) :
  subitem={}
  temp=data.replace('\n','').split('||')
  data0=temp[0].split()
  tmstmp='{} {}'.format(data0[0],data0[1])
  subitem['timestamp']=tmstmp
  ser=data0[3].split('.')
  subitem['service']=ser[-1]
  try :
    for t in temp :
          if 'ClientTransactionId' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
          elif 'MSISDN' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
          elif 'TransactionId' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
          elif 'OfferCode' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
          elif 'Server Id' in t and  ':: Status code' not in data :
            sub2=t.strip().split('::')
            subitem['system_id']=sub2[2]
            subitem['password']=sub2[3]
            subitem['shortcode']=sub2[4]
            subitem['message']=sub2[6]
            subitem['source']=sub2[10]
          elif ':: Status code' in data :
            sub2=t.strip().split('::')
            stt_code=sub2[2].split()
            subitem['status_code']=stt_code[2]
            req=sub2[3].split()
            req_time='{} {}'.format(req[2],req[3])
            subitem['request_time']=req_time
            res=sub2[4].split()
            res_time='{} {}'.format(res[1][4:],res[2])
            subitem['respond_time']=res_time
    return subitem
  except Exception :
    return None


def ExtractUniv(data) :
  subitem={}
  temp=data.replace('\n','').split('||')
  data0=temp[0].split()
  tmstmp='{} {}'.format(data0[0],data0[1])
  subitem['timestamp']=tmstmp
  ser=data0[3].split('.')
  subitem['service']=ser[-1]
  try :
    subitem['data']=temp[1]
    return subitem
  except Exception :
    return None


def ExtractUniVal(data) :
  subitem={}
  temp=data.replace('\n','').split('||')
  data0=temp[0].split()
  tmstmp='{} {}'.format(data0[0],data0[1])
  subitem['timestamp']=tmstmp
  ser=data0[3].split('.')
  subitem['service']=ser[-1]
  try :
    for t in temp :
      if 'ClientTransactionId' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
      elif 'MSISDN' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
      elif 'TransactionId' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
      elif 'OfferCode' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
      elif 'KeyWord' in data :
            sub2=t.strip().split('::')
            subitem[sub2[0]]=sub2[1]
      elif ' Request Time' in t and 'Request Time::' in data :
            sub2=t.strip().split('::')
            subitem['request_time']=sub2[1]
      elif ' Response time' in t and 'Response time::' in data :
            sub2=t.strip().split('::')
            subitem['response_time']=sub2[1]
      elif 'Server Id::' in data :
            sub2=ExtractServId(data)
            subitem['remarks']=sub2
      elif 'OperationCode' in data :
            sub2=t.strip().split('::')
            subitem['operation_code']=sub2[1]
      elif 'HTTP Response' in data :
            sub2=t.strip().split('::')
            subitem['http_response']=sub2[1]
      elif 'statusCode' in data :
            sub2=t.strip().split('::')
            subitem['basiccause']=sub2[1]
      else :
            subitem['remarks']=t
    return subitem
  except Exception:
    return subitem


def ExtractServId(data_t):
  temp=data_t.split('::')
  if len(temp) == 2:
    txt=temp[1]
  elif len(temp) == 3:
    if 'Time taken for' in data_data :
        txt=temp[2]
    elif 'Calling for' in data_t:
        txt=temp[1]
    else :
        txt=temp[2]
  elif len(temp) > 3 :
    if 'OPERATION' in data_t:
        if len(temp) == 4:
          txt=temp[2]
        else :
          txt='OPERATION:{0}, internalcause:{1}'.format(temp[3],temp[5])
    elif '::Returning Response::' in data_t:
        temp2=data_t.split('::')
        txt=temp2[3]
    elif ':: Status code' in data_t and ':: req time' in data_t:
        sttcode=temp[2].split()
        temp_req=temp[3].split()
        reqtime='{0} {1}'.format(temp_req[2],temp_req[2])
        temp_res=temp[4].split()
        restime='{0} {1}'.format(temp_res[2],temp_res[2])
        txt='basiccause:{0} , request_time:{1} , response_time:{2}'.format(sttcode[2],reqtime,restime)
    elif '::Request Time :' in data_t and '::Response time:' in data_t and 'ErrorCode' in data_data :
        reqtime=temp[2].replace('::Response time:','')
        txt='internalcause:{0} , request_time:{1} , response_time:{2}'.format(temp[6],reqtime,temp[4])
    elif 'Populate' in data_data :
        temp2=data_t.split('::')
        popstart=temp2[2][21:]
        txt='populate start:{0} , populate end:{1}'.format(popstart,temp2[4])
    else :
        txt=data_t
  return txt


def ExtractDict(data):
  list_item=[]
  if isinstance(data,dict) :
    for key in data :
      if key == 'timestamp' or key == 'service' :
        pass
      else :
        temp='{0}={1}'.format(key,data[key])
        list_item.append(temp)
      temp_txt=' , '.join(list_item)
    txt='{0} {1} --> {2}'.format(data.get('timestamp'),data.get('service'),temp_txt)
  else:
    txt=data
  return txt    

def ExtractScmLog(data):
   try :
      if 'BulkMTAdaptor' in data :
         data_temp=ExtractBulkMTAdaptor(data)
      elif 'MsisdnHashing' in data :
         data_temp=ExtractUniv(data)
      elif 'ProfilerCheckBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'PopulateAndValidateRequest' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'ValidateContentProvider' in data :
         data_temp=ExtractUniVal(data)
      elif 'SubscriptionDAO' in data :
         data_temp=ExtractUniv(data)
      elif 'CheckSubscriptionBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'BulkMTRequestBO' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'BulkChargingProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'CBSReserveBO' in t and 'ClientTransactionId' in data :
         data_temp=ExtractUniVal(data)
      elif 'URLRequestResponse' in data :
         data_temp=ExtractUniVal(data)
      elif 'MessageFormater' in data :
         data_temp=ExtractUniVal(data)
      elif 'CDRGeneratorBO' in data :
         data_temp=ExtractUniv(data)
      elif 'CBSDebitBO' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'PopulateRequestResponseBO' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'ChargingActionPoolBO' in data :
         data_temp=ExtractUniv(data)
      elif 'BulkMOActivationRequestBO' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'CommonValidationBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'ValidateTraiConfirmation' in data :
         data_temp=ExtractUniv(data)
      elif 'StoreBillingData' in data :
         data_temp=ExtractUniv(data)
      elif 'BulkMOAsynchProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'DeliveryReceiptBO' in data :
         data_temp=ExtractUniv(data)
      elif 'BulkMTProcessBO' in data :
         n=t.replace('\n','').split('||')
         if len(n) > 2 :
            data_temp=ExtractUniVal(data)
         else :
            data_temp=ExtractUniv(data)
      elif 'SmsSenderBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'BulkMTChargingProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'DeliveryOperationProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'OndemandDAO' in data :
         data_temp=ExtractUniv(data)
      elif 'MessageSenderBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'RequestProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'DeliveryReceiptUpdationBO' in data :
         data_temp=ExtractUniv(data)
      elif 'DeliveryReceiptAdaptor' in data :
         data_temp=ExtractUniVal(data)
      elif 'BulkMOProcessBO' in data :
         data_temp=ExtractUniVal(data)
      elif 'BulkMOAdaptor' in data :
         data_temp=ExtractUniVal(data)
      else :
         data_temp=ExtractUniv(data)
   except Exception :
      data_temp=None 
   if data_temp != None :
      return data_temp