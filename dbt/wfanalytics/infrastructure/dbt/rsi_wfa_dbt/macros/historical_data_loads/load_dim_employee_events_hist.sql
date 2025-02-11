{% macro load_dim_employee_events_hist() %}

    {% do log("Creating historical load for dim employee events", info=True) %}

        {% set sql %}

-- Create table
create or replace TABLE {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS (
    EMP_EVENT_SK VARCHAR(32)
  , CHANGE_COLUMN VARCHAR(16777216)
  , EIN VARCHAR(16777216)
  , CURRENT_VALUE VARCHAR(16777216) WITH MASKING POLICY CORE.DIM_EMPLOYEE_EVENTS_MASKING USING(CURRENT_VALUE,CHANGE_COLUMN)
  , PRIOR_VALUE VARCHAR(16777216) WITH MASKING POLICY CORE.DIM_EMPLOYEE_EVENTS_MASKING USING(PRIOR_VALUE,CHANGE_COLUMN)
  , NEXT_VALUE VARCHAR(16777216) WITH MASKING POLICY CORE.DIM_EMPLOYEE_EVENTS_MASKING USING(NEXT_VALUE,CHANGE_COLUMN)
  , EFF_DT DATE
  , DISC_DT DATE
  , IS_CURRENT NUMBER(38,0)
  , IS_DELETED NUMBER(1,0)
  , DELETED_DT DATE
  , CHANGE_TYPE VARCHAR(16777216)
);

insert into {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS(emp_event_sk,change_column,ein,current_value,eff_dt,prior_value)
with emp as(
  SELECT
    distinct ein
  , Change_column
  , current_value
  , rpt_effective_dt
  , original_hire_dt_1 as original_hire_dt
  FROM
  ( select
      distinct ein
    , rpt_effective_dt
    , original_hire_dt as original_hire_dt_1
    , termination_dt as termination_dt_1
    , WORKER
    , WORKER_TYPE
    , STATUS
    , ACTIVE_STATUS
    , 'CC_'|| trim(split_part(COST_CENTER_ID,'-',1)) 
      || trim(split_part(COST_CENTER_ID,'-',2))
      || trim(split_part(COST_CENTER_ID,'-',3)) as COST_CENTER_CD
    , DEFAULT_WEEKLY_HOURS
    , EMAIL_WORK
    , EMPLOYEE_TYPE
    , FTE_PERCENT
    , FULL_LEGAL_NAME
    , cast(coalesce(HIRE_DT, '1900-01-01') as varchar) as HIRE_DT
    , IS_REHIRE
    , JOB_CODE
    , cast(LAST_DAY_OF_WORK_DT as varchar) as LAST_DAY_OF_WORK_DT
    , LEGAL_FIRST_NAME
    , LEGAL_LAST_NAME
    , LEGAL_NAME_IS_PREFERRED_NAME
    , MANAGER_ID
    , cast(ORIGINAL_HIRE_DT as varchar) as ORIGINAL_HIRE_DT
    , PAY_GROUP
    , PAY_RATE_TYPE
    , cast(PAY_THROUGH_DT as varchar) as PAY_THROUGH_DT
    , POSITION_ID
    , PREFERRED_NAME
    , PREFERRED_NAME_FIRST_NAME
    , PREFERRED_NAME_LAST_NAME
    , USER_NAME
    , TERMINATION_REASON_SENS
    , TIME_TYPE
    , UNION_ID
    , TOTAL_PAY_AMT_SENS
    , CAST(CONTINUOUS_SERVICE_DT_SENS AS VARCHAR) as CONTINUOUS_SERVICE_DT_SENS
    , OTHER_IDS_PII
    , CASE WHEN UPPER(OTHER_IDS_PII) LIKE '%ACQUIRED COMPANY%'
        THEN SPLIT_PART(SPLIT_PART(OTHER_IDS_PII, 'Acquired Company/', 2),';',1)
      ELSE NULL END as ACQUISITION_ID
    from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }}
    qualify row_number() over(partition by ein, rpt_effective_dt order by rpt_effective_dt desc, ACTIVE_STATUS desc)= 1
  ) a UNPIVOT (current_value FOR change_column IN (
      WORKER, WORKER_TYPE, STATUS, ACTIVE_STATUS,
      COST_CENTER_CD, DEFAULT_WEEKLY_HOURS,
      EMAIL_WORK, EMPLOYEE_TYPE, FTE_PERCENT,
      FULL_LEGAL_NAME, HIRE_DT, IS_REHIRE,
      JOB_CODE, LAST_DAY_OF_WORK_DT, LEGAL_FIRST_NAME,
      LEGAL_LAST_NAME, LEGAL_NAME_IS_PREFERRED_NAME,
      MANAGER_ID, ORIGINAL_HIRE_DT, PAY_GROUP,
      PAY_RATE_TYPE, PAY_THROUGH_DT, POSITION_ID,
      PREFERRED_NAME, PREFERRED_NAME_FIRST_NAME,
      PREFERRED_NAME_LAST_NAME, USER_NAME,
      TERMINATION_REASON_SENS, TIME_TYPE, UNION_ID,
      TOTAL_PAY_AMT_SENS, CONTINUOUS_SERVICE_DT_SENS,
      OTHER_IDS_PII, ACQUISITION_ID
    )
  )
),
emp_unpivot as (
  select *, LAG(current_value) OVER (partition by ein, Change_column ORDER BY rpt_effective_dt) prior_value
  from emp
),
emp_term as (
  select emp1.ein
  , 'TERMINATION_DT' as CHANGE_COLUMN
  , coalesce(cast(emp1.termination_dt as varchar),'1900-01-01') as CURRENT_VALUE
  , cast(case when emp1.termination_dt is null then emp1.rpt_effective_dt else emp1.termination_dt end as varchar) as RPT_EFFECTIVE_DT
  , NULL as ORIGINAL_HIRE_DT
  , cast(LAG(coalesce(emp1.termination_dt,'1900-01-01'))
      OVER (partition by emp1.ein ORDER BY
        case when emp1.termination_dt is null then emp1.rpt_effective_dt else emp1.termination_dt end) as varchar) as PRIOR_VALUE
  from
  ( select ein,termination_dt,rpt_effective_dt
    from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }}
    where termination_dt is null
    and rpt_effective_dt <= '2022-11-02'
    union
    select ein,termination_dt,rpt_effective_dt
    from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }}
    where termination_dt is not null
    and rpt_effective_dt <= '2022-11-02'
    qualify row_number() over(partition by ein order by rpt_effective_dt desc)=1
  ) emp1
  left join
  ( select ein,termination_dt
    from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }}
    where termination_dt is not null and rpt_effective_dt <= '2022-11-02'
    qualify row_number() over(partition by ein order by rpt_effective_dt desc)=1
  ) emp2 on emp1.ein=emp2.ein
    and ( emp1.termination_dt is not null
        or emp1.rpt_effective_dt<emp2.termination_dt
        or coalesce(emp2.termination_dt,'1900-01-01')='1900-01-01') 
),
a as (
  select * from emp_unpivot where (current_value <> prior_value or prior_value is null)
  union
  select * from emp_term where (current_value <> prior_value  or prior_value is null)
)
select
  md5(cast( coalesce(cast(Change_Column as varchar), '') || '-' ||
            coalesce(cast(EIN as varchar), '') || '-' ||
            coalesce(cast(RPT_EFFECTIVE_DT as varchar), '')as varchar)) AS EMP_EVENT_SK
, Change_column
, ein
, case when Change_column in ('TERMINATION_DT', 'HIRE_DT')
    then case when CURRENT_VALUE = '1900-01-01'
          then null 
          else CURRENT_VALUE end
  else CURRENT_VALUE
  end as CURRENT_VALUE
, case when Change_column='TERMINATION_DT' and CURRENT_VALUE!='1900-01-01' then dateadd(day,+1,cast(CURRENT_VALUE as date))
       when PRIOR_VALUE is null and original_hire_dt < '2019-06-02' then '2019-06-02'
       when PRIOR_VALUE is null and original_hire_dt >= '2019-06-02' then original_hire_dt
  else RPT_EFFECTIVE_DT
  end RPT_EFFECTIVE_DT
, case when Change_column in ('TERMINATION_DT', 'HIRE_DT')
    then case when PRIOR_VALUE = '1900-01-01' then null else PRIOR_VALUE end
  else PRIOR_VALUE
  end as PRIOR_VALUE
from a
order by 2, 1, rpt_effective_dt;
 
UPDATE {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS d1
SET d1.disc_dt = d2.disc_dt_2
  , d1.next_value = d2.next_value_2
FROM
(
select *
  , dateadd(day, -1, lead(eff_dt) over (partition by ein,change_column order by eff_dt)) as disc_dt_2
  , lead(current_value) over(partition by ein,change_column order by eff_dt) as next_value_2
from {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS) d2
where d1.ein = d2.ein
and d1.change_column = d2.change_column
and d1.eff_dt = d2.eff_dt
and coalesce(d1.current_value,'') = coalesce(d2.current_value,'');
 
update {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS set disc_dt='9999-12-31',is_current=1 where disc_dt is null;
update {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS set is_current=0 where is_current is null;
update {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS set is_deleted=0 where is_deleted is null;
 
UPDATE {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS d1
SET d1.is_deleted = 1
  , d1.deleted_dt = dateadd(day,1,d2.rpt_effective_dt)
  , d1.CHANGE_TYPE = 'Employee_Deleted'
FROM
(
select EIN
  , rpt_effective_dt
from
( select ein
  , rpt_effective_dt
  , lead(rpt_effective_dt) OVER (partition by ein ORDER BY rpt_effective_dt) as next_rpt_effective_dt
  from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }}
) emp
where next_rpt_effective_dt is null
and rpt_effective_dt <> (select max(rpt_effective_dt) from {{ ref('vw_STG_WKD_EMPLOYEE_EXTRACT') }})
) d2
where d1.ein = d2.ein
and d1.is_current = 1
and d1.is_deleted = 0;
 
update {{target.database}}.CORE.DIM_EMPLOYEE_EVENTS set CHANGE_TYPE='Rescind' where eff_dt>disc_dt and change_type is null;

        {% endset %}
        
        {{ return(sql) }}

        {% do log("historical dim employee events table created", info=True) %}

{% endmacro %}
