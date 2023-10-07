SELECT to_char(timestamp,'HH24:MI') as cdrdate
,SUM(CASE WHEN accessflag='66' THEN 1 ELSE 0 END ) as bmo_attempt
,SUM(CASE WHEN accessflag='67' THEN 1 ELSE 0 END ) as bmt_attempt
,SUM(CASE WHEN accessflag='68' THEN 1 ELSE 0 END ) as dig_attempt
,SUM(CASE WHEN accessflag='72' THEN 1 ELSE 0 END ) as smo_attempt
,SUM(CASE WHEN accessflag='73' THEN 1 ELSE 0 END ) as smt_attempt
FROM scmcdr.scm_cc_{mon}
WHERE m_day='{day}' 
AND to_char(timestamp,'HH24:MI') >= '{tm1}'
AND to_char(timestamp,'HH24:MI') < '{tm2}'
AND accessflag in ('66','67','68','72','73')
GROUP BY to_char(timestamp,'HH24:MI')
ORDER BY to_char(timestamp,'HH24:MI')