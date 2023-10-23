SELECT TO_CHAR(timestamp,'YYYY-mm-dd HH24:MI') AS CDRDATE,CP_NAME,contentproviderid as CPID,ACCESSFLAG,BASICCAUSE,INTERNALCAUSE,COUNT(TRANSACTIONID) AS TOTAL
FROM SCMCDR.SCM_CC_{mon}
WHERE M_DAY ='{day}' AND ACCESSFLAG IN ('66','67','68','72','73')
AND TO_CHAR(timestamp,'HH24:MI') >= '{tm1}' AND TO_CHAR(timestamp,'HH24:MI') < '{tm2}'
GROUP BY TO_CHAR(timestamp,'YYYY-mm-dd HH24:MI'),CP_NAME,contentproviderid,ACCESSFLAG,BASICCAUSE,INTERNALCAUSE
ORDER BY TO_CHAR(timestamp,'YYYY-mm-dd HH24:MI'),contentproviderid,ACCESSFLAG