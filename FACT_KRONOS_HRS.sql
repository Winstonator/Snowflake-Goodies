{{
    config(
        materialized='incremental',
        tags=["core","fact","scheduled-daily"],
        unique_key='KRONOS_HRS_PK'
        )
}}

with 
kronos_agg as (
    select TIEMPN as EMP_ID
    , cast(substr(TIATIM, 1, 4) || '-' || substr(TIATIM, 5, 2) || '-' || 
            substr(TIATIM, 7, 2) as date) as SERVICE_PERIOD
    , substr(TIATIM, 1, 4) || '-' || substr(TIATIM, 5, 2) || '-' || substr(TIATIM, 7, 2) || ' ' ||
        substr(TIATIM, 9, 2) || ':' || substr(TIATIM, 11, 2) || ':' || substr(TIATIM, 13, 2) as PUNCH_IN_ACT
    , substr(TIATOM, 1, 4) || '-' || substr(TIATOM, 5, 2) || '-' || substr(TIATOM, 7, 2) || ' ' || 
        substr(TIATOM, 9, 2) || ':' || substr(TIATOM, 11, 2) || ':' || substr(TIATOM, 13, 2) as PUNCH_OUT_ACT
    , TISEC7
    , TIPC01
    , TIPC02
    , TIPC03
    , TIPC04
    , TIPC05
    , TIPC06
    , TIPC07
    , TIPC08
    , TIPC09
    , TIPC10
    , sum(TIHR01) / 100 as TIHR01
    , sum(TIHR02) / 100 as TIHR02
    , sum(TIHR03) / 100 as TIHR03
    , sum(TIHR04) / 100 as TIHR04
    , sum(TIHR05) / 100 as TIHR05
    , sum(TIHR06) / 100 as TIHR06
    , sum(TIHR07) / 100 as TIHR07
    , sum(TIHR08) / 100 as TIHR08
    , sum(TIHR09) / 100 as TIHR09
    , sum(TIHR10) / 100 as TIHR10
    From {{ source('STAGING', 'STG_WORKFORCE_KRONOS') }} k
    Group By 
    TIEMPN
    , cast(substr(TIATIM, 1, 4) || '-' || substr(TIATIM, 5, 2) || '-' ||
        substr(TIATIM, 7, 2) as date)
    , substr(TIATIM, 1, 4) || '-' || substr(TIATIM, 5, 2) || '-' || substr(TIATIM, 7, 2) || ' ' ||
        substr(TIATIM, 9, 2) || ':' || substr(TIATIM, 11, 2) || ':' || substr(TIATIM, 13, 2)
    , substr(TIATOM, 1, 4) || '-' || substr(TIATOM, 5, 2) || '-' || substr(TIATOM, 7, 2) || ' ' || 
        substr(TIATOM, 9, 2) || ':' || substr(TIATOM, 11, 2) || ':' || substr(TIATOM, 13, 2)
    , TISEC7
    , TIPC01
    , TIPC02
    , TIPC03
    , TIPC04
    , TIPC05
    , TIPC06
    , TIPC07
    , TIPC08
    , TIPC09
    , TIPC10
    Order By SERVICE_PERIOD, TIEMPN
)
, kronos_concat as (
    Select EMP_ID
    , to_date(service_period) as service_period_dt
    , cast(punch_in_act as datetime) as punch_in_ts
    , cast(punch_out_act as datetime) as punch_out_ts
    , TISEC7
    , TIPC01 || '::' || TIHR01 AS TI_01
    , TIPC02 || '::' || TIHR02 AS TI_02
    , TIPC03 || '::' || TIHR03 AS TI_03
    , TIPC04 || '::' || TIHR04 AS TI_04
    , TIPC05 || '::' || TIHR05 AS TI_05
    , TIPC06 || '::' || TIHR06 AS TI_06
    , TIPC07 || '::' || TIHR07 AS TI_07
    , TIPC08 || '::' || TIHR08 AS TI_08
    , TIPC09 || '::' || TIHR09 AS TI_09
    , TIPC10 || '::' || TIHR10 AS TI_10
    From kronos_agg    
)
, kronos_long as (
	Select emp_id
    , service_period_dt
    , punch_in_ts
    , punch_out_ts
    , tisec7
    , code_hrs
    , split_part(code_hrs, '::', 1) As paycode
    , split_part(code_hrs, '::', 2)::number(38, 6) As hrs
    From kronos_concat
    UNPIVOT (code_hrs For code_hrs_col In (ti_01,ti_02,ti_03,ti_04,ti_05,ti_06,ti_07,ti_08,ti_09,ti_10))
)
, kronos_long_clean as (
	Select emp_id
    , service_period_dt
    , punch_in_ts
    , punch_out_ts
    , paycode
    , hrs
    , Case When paycode In ('LDOT','LTDTY','P060R','PREM2','REG','RETN','GTY','SAFE'
                            ,'TRNP','TRVL','PREM3','REGCOVOTHE','REGCOVTEMP'
                            ,'REGCOVOTHER','REGSOS') Then 'REG'
            When paycode In ('DTC','OTC','DT','HL15N','OT','P090O','PR2OT','RG15'
                            ,'HL20N','HL25N','HLW15','HLWX2','PR3OT','DTCOVOTHER'
                            ,'DTCOVTEMP','OTCOVOTHER','OTCOVTEMP') Then 'OT'
            When paycode = 'INPA' Then 'INPA'
        Else 'ONP'
        End As paycode_type
    , TISEC7
    , split_part(TISEC7, '+', 1) As division
    , split_part(split_part(TISEC7, '+', 2), '-', 1) As local_use_cd
    , split_part(split_part(TISEC7, '+', 2), '-', 2) As paid_lob
    , split_part(TISEC7, '+', 3) As job_cd
    From kronos_long
    Where paycode is not null
	And hrs > 0
)
, kronos_wide as (
    Select emp_id
    , service_period_dt
    , punch_in_ts
    , punch_out_ts
    -- , tisec7
    , division
    , local_use_cd
    , paid_lob
    , job_cd
    , paycode
    , "'REG'" As reg_hrs
    , "'OT'" As ot_hrs
    , "'ONP'" As onp_hrs
    , "'INPA'" As inpa_hrs
    From kronos_long_clean
	PIVOT (sum(HRS) For paycode_type In ('REG', 'OT', 'ONP', 'INPA'))
)
, kronos_wide_raw as (
    select stg.*
    from kronos_wide stg
    {% if is_incremental() %}
    join (select emp_id as fact_emp_id, max(SERVICE_PERIOD_DT) as SERVICE_PERIOD_DT
            from {{ this }}
            group by emp_id) fact
    on fact.fact_emp_id = stg.emp_id --existing emp_ids
    where (stg.SERVICE_PERIOD_DT >= nvl(fact.SERVICE_PERIOD_DT, '1900-01-01'))
        or
        {{ dbt_utils.surrogate_key(['EMP_ID','PUNCH_IN_TS','DIVISION','LOCAL_USE_CD','PAID_LOB','JOB_CD','PAYCODE']) }}
        in (
        select KRONOS_HRS_PK from {{ this }}
        where  EMP_SK='-1' or ORG_SK='-1' or JOB_SK='-1')
    UNION ALL
    select stg.*
    from kronos_wide stg
    left join {{ this }} fact on fact.emp_id = stg.emp_id 
        and fact.SERVICE_PERIOD_DT = stg.SERVICE_PERIOD_DT --new emp_ids
    where fact.emp_id is null 
    {% endif %}
)
, dim_employee as (
    select emp_id, eff_dt, disc_dt, emp_sk from {{ ref('DIM_EMPLOYEE') }}
)
, dim_organization as (
    select division_cd, local_use_cd, department_cd, rpt_eff_dt, disc_dt, org_sk from {{ ref('DIM_ORGANIZATION') }}
)
, dim_job as (
    select job_cd, job_sk from {{ ref('DIM_JOB') }}
)
, final as (
    Select
      coalesce(emp.emp_sk,'-1') as EMP_SK
    , coalesce(job.job_sk,'-1') as JOB_SK
    , coalesce(org.org_sk,'-1') as ORG_SK
    , k.emp_id
    , k.service_period_dt
    , k.punch_in_ts
    , k.punch_out_ts
    , k.division
    , k.local_use_cd
    , k.paid_lob
    , k.job_cd
    , k.paycode
    , k.reg_hrs
    , k.ot_hrs
    , k.onp_hrs
    , k.inpa_hrs
    From kronos_wide_raw k
    left join dim_employee emp on k.emp_id = emp.emp_id
        and k.service_period_dt between to_date(emp.eff_dt) and to_date(emp.disc_dt)
    left join dim_organization org on org.division_cd = k.division 
        and org.local_use_cd = k.local_use_cd and org.department_cd = k.paid_lob
        and k.service_period_dt between to_date(org.rpt_eff_dt) and to_date(org.disc_dt)
    left join dim_job job on job.job_cd = k.job_cd
)
select 
  {{ dbt_utils.surrogate_key(['EMP_ID','PUNCH_IN_TS','DIVISION','LOCAL_USE_CD','PAID_LOB','JOB_CD','PAYCODE'])}} as KRONOS_HRS_PK
, emp_sk
, job_sk
, org_sk
, emp_id
, service_period_dt
, punch_in_ts
, punch_out_ts
, division
, local_use_cd
, paid_lob
, job_cd
, paycode
, reg_hrs
, ot_hrs
, onp_hrs
, inpa_hrs
from final


