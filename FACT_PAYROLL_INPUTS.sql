{{
    config(
        materialized='incremental',
        tags=["core","fact","scheduled-daily"],
        unique_key='PAYROLL_INPUT_PK'
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
        {{ dbt_utils.surrogate_key(['PI_WID']) }} 
        in (
        select PAYROLL_INPUT_PK from {{ this }}
        where  emp_sk='-1')
    UNION ALL
    select stg.*
    from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    left join {{ this }} fact on fact.emp_id = stg.employee_id
        and fact.PRL_PYMT_OR_REVERSAL_DT = stg.PRL_PAYMENT_OR_REVERSAL_DT --new emp_ids
    where fact.emp_id is null 
    {% endif %}
)
-- dedup step
,final as (
  select 
    Employee_ID,
    PR_WID,
    PRL_WID,
    PI_Batch_ID_SENS,
    PI_REFID,
    PI_Ongoing_Or_Onetime,
    PI_Start_DT,
    PI_End_DT,
    PI_WID,
    PI_Last_Upd_DTM,
    PI_Last_Functionally_Upd_DTM,
    PI_Coverage_Start_DT,
    PI_Coverage_End_DT,
    PI_AMT_SENS,
    PI_Allow_any_Pay_Component,
    PI_Comment,
    PI_Costing_Company,
    PI_Override_vs_Adjustment,
    PI_Pay_Code_SENS,
    PI_Related_Calc_With_Alt_ID,
    PI_Run_Category,
    PI_Run_Category_Type,
    PI_Used_in_Off_Cycle,
    PI_Used_in_On_Cycle,
    PI_Used_in_Retro,
    PRL_PAYMENT_OR_REVERSAL_DT,
  row_number() over(Partition by PI_WID order by PRL_PAYMENT_OR_REVERSAL_DT desc ) as rn
from raw_data
qualify rn = 1
order by EMPloyee_id 
) 
--intergrate with dimensions emplyoee
select 
    {{ dbt_utils.surrogate_key(['PI_WID']) }} as PAYROLL_INPUT_PK,
        coalesce(emp.EMP_SK,'-1') as EMP_SK,
        Employee_ID as EMP_ID,
        PR_WID as PR_WID,
        PRL_WID as PRL_WID,
        PI_WID as PI_WID,
        PI_Batch_ID_SENS as PI_BATCH_ID_SENS,
        PI_REFID as PI_REFID,
        PI_Ongoing_Or_Onetime as PI_ONGOING_OR_ONETIME,
        PI_Start_DT as PI_START_DT,
        PI_End_DT as PI_END_DT,
        PI_Last_Upd_DTM as PI_LAST_UPD_DTM,
        PI_Last_Functionally_Upd_DTM as PI_LAST_FUNCTIONALLY_UPD_DTM,
        PI_Coverage_Start_DT as PI_COVERAGE_START_DT,
        PI_Coverage_End_DT as PI_COVERAGE_END_DT,
        PI_AMT_SENS as PI_AMT_SENS,
        PI_Allow_any_Pay_Component as PI_ALLOW_ANY_PAY_COMPONENT,
        PI_Comment as PI_COMMENT,
        PI_Costing_Company as PI_COSTING_COMPANY,
        PI_Override_vs_Adjustment as PI_OVERRIDE_VS_ADJUSTMENT,
        PI_Pay_Code_SENS as PI_PAY_CODE_SENS,
        PI_Related_Calc_With_Alt_ID as PI_RELATED_CALC_WITH_ALT_ID,
        PI_Run_Category as PI_RUN_CATEGORY,
        PI_Run_Category_Type as PI_RUN_CATEGORY_TYPE,
        PI_Used_in_Off_Cycle as PI_USED_IN_OFF_CYCLE,
        PI_Used_in_On_Cycle as PI_USED_IN_ON_CYCLE,
        PI_Used_in_Retro as PI_USED_IN_RETRO,
        PRL_PAYMENT_OR_REVERSAL_DT as PRL_PYMT_OR_REVERSAL_DT,
    (select to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss'))) as INS_BATCH_ID
from final f
    left join {{ ref('DIM_EMPLOYEE') }} emp 
        on f.Employee_ID=emp.EMP_ID 
         and to_date(f.PRL_PAYMENT_OR_REVERSAL_DT) between to_date(emp.eff_dt) and to_date(emp.disc_dt)
