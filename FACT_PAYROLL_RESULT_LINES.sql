{{
    config(
        materialized='incremental',
        tags=["core","fact","scheduled-daily"],
        unique_key='PAYROLL_RESULT_LINES_PK'
        )
}}
-- get raw data from staging
with
raw_data as (
    select stg.*
    from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    {% if is_incremental() %}
    join (select emp_id, max(PRL_PYMT_OR_REVERSAL_DT) PRL_PYMT_OR_REVERSAL_DT
            from {{ this }}
            group by emp_id) fact
    on fact.emp_id = stg.employee_id --existing emp_ids
    where (stg.PRL_PAYMENT_OR_REVERSAL_DT >= nvl(fact.PRL_PYMT_OR_REVERSAL_DT, '1900-01-01'))
        or
        {{ dbt_utils.surrogate_key(['PRL_WID']) }}
        in (
        select PAYROLL_RESULT_LINES_PK from {{ this }}
        where  emp_sk='-1' or JOB_CD_SK='-1' or PAY_CD_SK ='-1' or ORG_SK='-1' or LOCATION_SK='-1')
    UNION ALL
    select stg.*
    from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    left join {{ this }} fact on fact.emp_id = stg.employee_id 
        and fact.PRL_PYMT_OR_REVERSAL_DT = stg.PRL_PAYMENT_OR_REVERSAL_DT --new emp_ids
    where fact.emp_id is null 
    {% endif %}
)
, final as (
select 
    PR_WID,
    Employee_ID,
    PRL_Job_Code_From_Worktag,
    PRL_Payment_or_Reversal_DT,
    PRL_Reversal_Result,
    PRL_Retro_Period,
    PRL_Sub_Period,
    PRL_Payroll_Worktags_SENS,
    PRL_Result_Line_AMT_SENS,
    PRL_Unprorated_HRS_SENS,
    PRL_RLC_Worked_HRS_SENS,
    PRL_RLC_Scheduled_HRS_SENS,
    PRL_Result_Line_HRS_SENS,
    PRL_RLC_Taxable_HRS_SENS,
    PRL_RLC_Total_HRS_SENS,
    PRL_Pay_Code_SENS,
    PRL_Period_Start_DT,
    PRL_Period_End_DT,
    PRL_Cost_Center_ID,
    PRL_Company_ID,
    PRL_Location_ID,
    PRL_Journal_Entries_SENS,
    PRL_WID,
    PRL_Last_Functionally_Upd_DTM,
    row_number() over(Partition by PRL_WID order by PRL_Payment_or_Reversal_DT desc ) as rn
from raw_data
qualify rn = 1
order by EMPloyee_id 
) 
--intergrate with dimensions emplyoee and position
select 
    {{ dbt_utils.surrogate_key(['PRL_WID'])}} as PAYROLL_RESULT_LINES_PK,
    coalesce(emp.emp_sk,'-1') as EMP_SK,
    coalesce(j.job_sk,'-1') as JOB_CD_SK,
    pc.PAY_CD_SK as PAY_CD_SK,
    org.ORG_SK as ORG_SK,
    '-1' as LOCATION_SK,    
    PRL_WID as PRL_WID,
    Employee_ID as EMP_ID,
    PR_WID as PR_WID,
    PRL_Job_Code_From_Worktag as PRL_JOB_CD_FROM_WORKTAG,
    PRL_Payment_or_Reversal_DT as PRL_PYMT_OR_REVERSAL_DT,
    PRL_Reversal_Result as PRL_REVERSAL_RESULT,
    PRL_Retro_Period as PRL_RETRO_PERIOD,
    PRL_Sub_Period as PRL_SUB_PERIOD,
    PRL_Payroll_Worktags_SENS as PRL_PAYROLL_WORKTAGS_SENS,
    PRL_Result_Line_AMT_SENS as PRL_RESULT_LINE_AMT_SENS,
    PRL_Unprorated_HRS_SENS as PRL_UNPRORATED_HRS_SENS,
    PRL_RLC_Worked_HRS_SENS as PRL_RLC_WORKED_HRS_SENS,
    PRL_RLC_Scheduled_HRS_SENS as PRL_RLC_SCHEDULED_HRS_SENS,
    PRL_Result_Line_HRS_SENS as PRL_RESULT_LINE_HRS_SENS,
    PRL_RLC_Taxable_HRS_SENS as PRL_RLC_TAXABLE_HRS_SENS,
    PRL_RLC_Total_HRS_SENS as PRL_RLC_TOTAL_HRS_SENS,
    PRL_Pay_Code_SENS as PRL_PAYCODE_SENS,
    PRL_Period_Start_DT as PRL_PERIOD_START_DT,
    PRL_Period_End_DT as PRL_PERIOD_END_DT,
    PRL_Cost_Center_ID as PRL_COST_CENTER_ID,
    PRL_Company_ID as PRL_COMPANY_ID,
    PRL_Location_ID as PRL_LOCATION_ID,
    PRL_Journal_Entries_SENS as PRL_JOURNAL_ENTRIES_SENS,
    PRL_Last_Functionally_Upd_DTM as PRL_LAST_FUNCTIONALLY_UPD_DTM,
    (select to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss'))) as ins_batch_id
from final f
    left join {{ ref('DIM_EMPLOYEE') }} emp 
        on f.Employee_ID=emp.EMP_ID 
         and to_date(f.PRL_PAYMENT_OR_REVERSAL_DT) between to_date(emp.eff_dt) and to_date(emp.disc_dt)
    left join {{ ref('DIM_JOB') }} j
        on f.PRL_Job_Code_From_Worktag = j.job_cd
    left join {{ ref('DIM_PAYCODES') }} pc
        on f.PRL_Pay_Code_SENS = pc.pay_cd
    left join {{ ref('DIM_ORGANIZATION') }} org
        on f.PRL_COST_CENTER_ID = org.COST_CENTER_ID
