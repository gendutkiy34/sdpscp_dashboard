SELECT to_char(CDR_TIMESTAMP,'dd-mm-yyyy') CDR_DATE,to_char(CDR_TIMESTAMP,'HH24:MI') CDR_HOUR
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' THEN 1 ELSE 0 END) AS MM_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS MM_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='1' THEN 1 ELSE 0 END) AS MM_MOC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='1' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS MM_MOC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='2' THEN 1 ELSE 0 END) AS MM_MTC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='2' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS MM_MTC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='3' THEN 1 ELSE 0 END) AS MM_MFC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='3' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS MM_MFC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='4' THEN 1 ELSE 0 END) AS MM_UCB_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND CALL_TYPE='4' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS MM_UCB_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND  ISBFT='1' THEN 1 ELSE 0 END) AS MM_BFT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='MM' AND  IS_CHARGING_OVERRULED='1' THEN 1 ELSE 0 END) AS MM_BYPASS
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' THEN 1 ELSE 0 END) AS PK_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS PK_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='1' THEN 1 ELSE 0 END) AS PK_MOC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='1' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS PK_MOC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='2' THEN 1 ELSE 0 END) AS PK_MTC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='2' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS PK_MTC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='3' THEN 1 ELSE 0 END) AS PK_MFC_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='3' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS PK_MFC_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='4' THEN 1 ELSE 0 END) AS PK_UCB_ATT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND CALL_TYPE='4' AND DIAMETER_RESULT_CODES IN ('2001','4010','4012','5030','5031') THEN 1 ELSE 0 END) AS PK_UCB_SUC
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND  ISBFT='1' THEN 1 ELSE 0 END) AS PK_BFT
,SUM(CASE WHEN SUBSTR(INSTANCE_ID,0,2)='PK' AND  IS_CHARGING_OVERRULED='1' THEN 1 ELSE 0 END) AS PK_BYPASS
FROM
(SELECT CDR_TIMESTAMP,CALL_REFERENCE_NUMBER,INSTANCE_ID,DIAMETER_RESULT_CODES,CALL_TYPE,IS_ROAMING,IS_CHARGING_OVERRULED,ISBFT
FROM SCPCDR.INTERNAL_CDR_{day}
WHERE M_MONTH= '{mon}'
UNION ALL
SELECT CDR_TIMESTAMP,CALL_REFERENCE_NUMBER,INSTANCE_ID,DIAMETER_RESULT_CODES,CALL_TYPE,IS_ROAMING,IS_CHARGING_OVERRULED,ISBFT
FROM SCPCDR.INTERNAL_CDR_{day}@scpcdr_prod_pk_public
WHERE M_MONTH= '{mon}' 
)
WHERE to_char(CDR_TIMESTAMP,'HH24:MI') >= '{min1}' AND to_char(CDR_TIMESTAMP,'HH24:MI') <'{min2}'
GROUP BY to_char(CDR_TIMESTAMP,'dd-mm-yyyy') ,to_char(CDR_TIMESTAMP,'HH24:MI')
ORDER BY to_char(CDR_TIMESTAMP,'HH24:MI')