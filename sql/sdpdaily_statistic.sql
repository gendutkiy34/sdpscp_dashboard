SELECT to_char(TIMESTAMP,'dd-mm-yyyy') CDR_DATE,BASICCAUSE ,INTERNALCAUSE ,ACCESSFLAG,CONTENTPROVIDERID,CP_NAME 
,COUNT(TRANSACTIONID ) TOTAL
FROM scmcdr.scm_cc_{mon}
WHERE m_day='{day}' 
AND ACCESSFLAG IN ('66','67','68','72','73','1','8')
GROUP BY to_char(TIMESTAMP,'dd-mm-yyyy'),BASICCAUSE ,INTERNALCAUSE ,ACCESSFLAG,CONTENTPROVIDERID,CP_NAME