SELECT to_char(TIMESTAMP,'dd-mm-yyyy HH24') CDR_DATE,BASICCAUSE ,INTERNALCAUSE ,ACCESSFLAG,CONTENTPROVIDERID,CP_NAME 
,COUNT(TRANSACTIONID ) TOTAL
FROM scmcdr.scm_cc_{mon}
WHERE m_day='{day}' 
AND ACCESSFLAG IN ('66','67','68','72','73')
GROUP BY to_char(TIMESTAMP,'dd-mm-yyyy HH24'),BASICCAUSE ,INTERNALCAUSE ,ACCESSFLAG,CONTENTPROVIDERID,CP_NAME