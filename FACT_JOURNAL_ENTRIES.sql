
{{
    config(
        materialized='incremental',
        unique_key='JOURNAL_ENTRIES_PK'
        )
}}
-- get raw data from staging
with
raw_data as (
    select stg.* from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    {% if is_incremental() %}
    join (select emp_id, max(PRL_PYMT_OR_REVERSAL_DT) PRL_PYMT_OR_REVERSAL_DT
            from {{ this }}
            group by emp_id) fact
    on fact.emp_id = stg.employee_id --existing emp_ids
    where (stg.PRL_PAYMENT_OR_REVERSAL_DT >= nvl(fact.PRL_PYMT_OR_REVERSAL_DT, '1900-01-01'))
        or
        {{ dbt_utils.surrogate_key(['Employee_ID',	'pr_wid',	'prl_journal_entries_sens',	'prl_wid']) }}
        in (
        select JOURNAL_ENTRIES_PK from {{ this }}
        where  emp_sk='-1')
    UNION ALL
    select stg.*
    from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    left join {{ this }} fact on fact.emp_id = stg.employee_id
        and fact.PRL_PYMT_OR_REVERSAL_DT = stg.PRL_PAYMENT_OR_REVERSAL_DT --new emp_ids
    where fact.emp_id is null
    {% endif %}
)
--dedup step
, dedup_data as (
select 
    employee_id,
    pr_wid,
    prl_journal_entries_sens,
    prl_wid,
    PRL_Last_Functionally_Upd_DTM,
    PRL_PAYMENT_OR_REVERSAL_DT,
    row_number() over(Partition by employee_id,	pr_wid,	prl_journal_entries_sens, prl_wid order by PRL_PAYMENT_OR_REVERSAL_DT desc ) as rn
from raw_data
qualify rn = 1
)
, final as (
  select 
    employee_id
    , pr_wid
    , prl_wid
    , PRL_Last_Functionally_Upd_DTM
    , PRL_PAYMENT_OR_REVERSAL_DT
    , trim(to_varchar(value)) as prl_journal_entries_sens
  from dedup_data, lateral flatten(input => split(prl_journal_entries_sens, ';')) j
)
--intergrate with dimensions emplyoee and position
select 
    {{ dbt_utils.surrogate_key(['Employee_ID',	'pr_wid',	'prl_journal_entries_sens',	'prl_wid' ]) }} as JOURNAL_ENTRIES_PK,
    coalesce(emp.emp_sk,'-1') as EMP_SK,
    employee_id as EMP_ID,
    pr_wid as PR_WID,
    prl_journal_entries_sens as PRL_JOURNAL_ENTRIES_SENS,
    prl_wid as PRL_WID,
    PRL_Last_Functionally_Upd_DTM as PRL_LAST_FUNCTIONALLY_UPD_DTM,
    PRL_PAYMENT_OR_REVERSAL_DT as PRL_PYMT_OR_REVERSAL_DT,
    (select to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss'))) as ins_batch_id
from final f
    left join {{ ref('DIM_EMPLOYEE') }} emp 
        on f.Employee_ID=emp.EMP_ID 
         and to_date(f.PRL_PAYMENT_OR_REVERSAL_DT) between to_date(emp.eff_dt) and to_date(emp.disc_dt)
