import json


def ExtractCdrSdp(listdict=None):
    list_clean=[]
    for d in listdict:
        newdict={}
        for k in d:
            if k == "THIRDPARTYERRORCODE" :
                try :
                    temp=d[k].split(';')
                    for t in temp :
                        if 'RESERVE_BALANCE_CBS_ERRORCODE' in t :
                            temp2=t.split('=')
                            newdict['RESERVE'] = temp2[1]
                        elif 'CP_NOTIFICATION_ERRORCODE' in t :
                            temp2=t.split('=')
                            newdict['CP_RESPOND_CODE'] = temp2[1]
                        elif 'DEBIT_BALANCE_CBS_ERRORCODE' in t :
                            temp2=t.split('=')
                            newdict['COMMIT'] = temp2[1]
                except Exception :
                    if 'RESERVE_BALANCE_CBS_ERRORCODE' in t :
                            temp2=t.split('=')
                            newdict['RESERVE'] = temp2[1]
            elif d[k] == None :
                newdict[k]="-"
            else :
                newdict[k]=d[k]
        list_clean.append(newdict)
    return list_clean
    
        