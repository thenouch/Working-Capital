create or replace table "EVALUATION_1"."RPT"."Working_Capital" (
 account string,
date date,
daily_balance float)

set max_date = (select max(TO_DATE(date)) from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE");
set min_date = (select min(TO_DATE(date)) from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE");
  

--insert into  "EVALUATION_1"."RPT"."Working_Capital" (account,date,daily_balance) 
-- pulls min & max date by Account
with cte_daterange AS (
select t1.ACCOUNT ,
 MIN(TO_DATE(t1.DATE)) as MinDate
, MAX(TO_DATE(t1.DATE)) as MaxDate
  from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE" as t1
GROUP BY t1.ACCOUNT),
  --select * from cte_daterange
 -- gives all dates between start & end of accounts receiveable 
  cte_dates AS (
  select cte_daterange.Account, d.MY_DATE
  from cte_daterange inner join
  "EVALUATION_1"."RPT"."DATE_TABLE1" d 
    ON d.MY_DATE >= $min_date AND d.MY_DATE <= $max_date)
--select * from cte_dates
, 
-- daily credit & debit from AR
cte_dailycosts as (
select t1.Account, TO_DATE(t1.DATE) as t_date, SUM(DEBIT) AS DebitDaily, 
 SUM(CREDIT) as CreditDaily,DebitDaily - CreditDaily as Running_total
from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE" t1 GROUP BY
t1.Account, TO_DATE(t1.DATE)),
--select * from cte_dailycosts
-- provides rolling sum 
cte_sum as (
select cte_dates.Account,
cte_dates.MY_DATE as all_dates,
cte_dailycosts.Running_total,
SUM(cte_dailycosts.running_total) OVER(PARTITION BY cte_dates.Account ORDER BY cte_dates.MY_DATE rows between unbounded preceding and current row) AS rollingSum
FROM cte_dates full outer join cte_dailycosts ON 
  cte_dailycosts.Account = cte_dates.Account AND cte_dailycosts.t_date = cte_dates.MY_DATE
)

--select * from cte_sum
--cte_sum.all_dates = DATEADD(day,-1,cte_sum.all_dates)


select  distinct cte_sum.Account,
        cte_sum.all_dates as DATE,
        case 
        when cte_sum.rollingSum is NULL then (select distinct last_value(t1.rollingSum) ignore nulls 
  OVER(PARTITION BY t1.Account order by t1.all_dates  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rollingSum 
  from cte_sum t1 limit 1)
        else cte_sum.rollingSum end as rollingSum
 from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE" as t1
 full outer join cte_sum ON
 cte_sum.Account = t1.Account and cte_sum.all_dates = TO_DATE(t1.DATE) 
 --where cte_sum.Account = 'Account2'
 order by DATE asc, cte_sum.Account asc 
  


--select * from "EVALUATION_1"."RPT"."Working_Capital"
--drop table "EVALUATION_1"."RPT"."Working_Capital"






-- NOT used in final output ---

--- approach #2
 --set max_date = (select max(TO_DATE(date)) from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE");
--set min_date = (select min(TO_DATE(date)) from "EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE");
  


 --insert into  "EVALUATION_1"."RPT"."Working_Capital"(
--select t1.MY_DATE, t2.account, t2.balance from "EVALUATION_1"."RPT"."DATE_TABLE1" t1 full outer join (
--select distinct account,TO_DATE(Date) as DATE1, SUM(IFNULL(DEBIT,0)) OVER (PARTITION BY ACCOUNT order by Date1) as running_debit, 
--  sum(IFNULL(Credit,0)) OVER(PARTITION BY ACCOUNT order by Date1) as running_credit,
--  SUM(IFNULL(DEBIT,0)) OVER (PARTITION BY ACCOUNT order by Date1) - sum(IFNULL(Credit,0)) OVER(PARTITION BY ACCOUNT order by Date1) as balance from
--"EVALUATION_1"."ETL"."ACCOUNTS_RECEIVABLE" ) t2 on t1.MY_DATE = t2.DATE1 where MY_DATE between $min_date and $max_date
--order by MY_DATE desc
