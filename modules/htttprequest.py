import os
import requests


def ReqHttp(urllink):
    try :
        r=requests.get(urllink)
        respcode=r.status_code
        resptext=r.text
    except Exception :
        respcode='404'
        resptext='test failed'
    return respcode,resptext




