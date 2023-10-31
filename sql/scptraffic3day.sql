SELECT *
FROM
(SELECT to_char(CDR_TIMESTAMP,'dd-mm-yyyy HH24')  AS CDRDATE,SERVICE_KEY,IS_ROAMING,DIAMETER_RESULT_CODES AS DIAMETER,COUNT(TRANSACTION_ID) AS total
FROM SCPCDR.INTERNAL_CDR_{day}
WHERE M_MONTH= '{mon}' AND  to_char(CDR_TIMESTAMP,'HH24:MI') < '{hourmin}'
GROUP BY to_char(CDR_TIMESTAMP,'dd-mm-yyyy HH24'),SERVICE_KEY,IS_ROAMING,DIAMETER_RESULT_CODES
UNION ALL
SELECT to_char(CDR_TIMESTAMP,'dd-mm-yyyy HH24') AS CDRDATE,SERVICE_KEY,IS_ROAMING,DIAMETER_RESULT_CODES AS DIAMETER,COUNT(TRANSACTION_ID) AS total
FROM SCPCDR.INTERNAL_CDR_{day}@scpcdr_prod_pk_public
WHERE M_MONTH= '{mon}' AND  to_char(CDR_TIMESTAMP,'HH24:MI') < '{hourmin}'
GROUP BY to_char(CDR_TIMESTAMP,'dd-mm-yyyy HH24'),SERVICE_KEY,IS_ROAMING,DIAMETER_RESULT_CODES )
ORDER BY CDRDATE,SERVICE_KEY,DIAMETER
