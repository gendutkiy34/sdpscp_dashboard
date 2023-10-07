SELECT to_char(timestamp,'dd-mm-yyyy') as cdrdate,hour
,SUM(CASE WHEN accessflag='66' THEN 1 ELSE 0 END ) as bmo_attempt
,SUM(CASE WHEN accessflag='67' THEN 1 ELSE 0 END ) as bmt_attempt
,SUM(CASE WHEN accessflag='68' THEN 1 ELSE 0 END ) as dig_attempt
,SUM(CASE WHEN accessflag='72' THEN 1 ELSE 0 END ) as smo_attempt
,SUM(CASE WHEN accessflag='73' THEN 1 ELSE 0 END ) as smt_attempt
,SUM(CASE WHEN accessflag='66' AND internalcause in ('2001','4010','4012','5031','5030') THEN 1 ELSE 0 END ) as bmo_success
,SUM(CASE WHEN accessflag='67' AND internalcause in ('2001','4010','4012','5031','5030') THEN 1 ELSE 0 END ) as bmt_success
,SUM(CASE WHEN accessflag='68' AND internalcause in ('2001','4010','4012','5031','5030') THEN 1 ELSE 0 END ) as dig_success
,SUM(CASE WHEN accessflag='72' AND internalcause in ('2001','4010','4012','5031','5030') THEN 1 ELSE 0 END ) as smo_success
,SUM(CASE WHEN accessflag='73' AND internalcause in ('2001','4010','4012','5031','5030') THEN 1 ELSE 0 END ) as smt_success
FROM scmcdr.scm_cc_{mon}
WHERE m_day='{day}' and hour <= to_char(sysdate,'HH24') and accessflag in ('66','67','68','72','73')
GROUP BY to_char(timestamp,'dd-mm-yyyy'),hour
ORDER BY hour