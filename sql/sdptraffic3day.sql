SELECT TO_CHAR(timestamp,'YYYY-mm-dd HH24') AS CDRDATE,CP_NAME,contentproviderid as CPID,ACCESSFLAG,BASICCAUSE,INTERNALCAUSE,SUM(CALLCHARGE ) AS REVENUE,COUNT(TRANSACTIONID) AS TOTAL
FROM SCMCDR.SCM_CC_{mon}
WHERE M_DAY ='{day}' AND ACCESSFLAG IN ('66','67','68','72','73')
AND TO_CHAR(timestamp,'HH24:MI') < '{hourmin}'
GROUP BY TO_CHAR(timestamp,'YYYY-mm-dd HH24'),CP_NAME,contentproviderid,ACCESSFLAG,BASICCAUSE,INTERNALCAUSE
ORDER BY TO_CHAR(timestamp,'YYYY-mm-dd HH24'),contentproviderid,ACCESSFLAG