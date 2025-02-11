
{{
    config(
        materialized='incremental',
        tags=["core","fact","scheduled-daily"],
        unique_key='PAYROLL_RESULT_PK'
        )
}}
-- get raw data from staging
with
raw_data as (
    select stg.* from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    {% if is_incremental() %}
    join (select emp_id, max(PR_PYMT_OR_REVERSAL_DT) PR_PYMT_OR_REVERSAL_DT
            from {{ this }}
            group by emp_id) fact
    on fact.emp_id = stg.employee_id --existing emp_ids
    where (stg.PR_PAYMENT_OR_REVERSAL_DT >= nvl(fact.PR_PYMT_OR_REVERSAL_DT, '1900-01-01'))
        or
        {{ dbt_utils.surrogate_key(['PR_WID'])}}
            in (select PAYROLL_RESULT_PK from {{ this }}
                where  position_sk='-1' or emp_sk='-1')
    UNION ALL
    select stg.*
    from {{ source('STAGING', 'STG_WKD_PAYROLL_RESULT_DETAIL') }} stg
    left join {{ this }} fact on fact.emp_id = stg.employee_id
        and fact.PR_PYMT_OR_REVERSAL_DT = stg.PR_PAYMENT_OR_REVERSAL_DT --new emp_ids
    where fact.emp_id is null 
    {% endif %}
),
position as (
    select position_id, position_sk, eff_dt, disc_dt
    from {{ ref('DIM_POSITION') }}
    qualify row_number() over (partition by position_id order by eff_dt desc) = 1
),
--dedup step
final as (
    select Employee_ID
    ,PR_Pay_Group_Detail
    ,PR_Payment_Type
    ,PR_Journal_DT
    ,PR_Federal_Marital_Status_SENS
    ,PR_Federal_Allowance_CNT_SENS
    ,PR_Federal_Allowance_Plus_SENS
    ,PR_State_Allowance_CNT_SENS
    ,PR_Period_Start_DT
    ,PR_GTN_Or_Period_End_DT
    ,PR_Payment_or_Reversal_DT
    ,PR_Gross_Pay_AMT_SENS
    ,PR_Net_Pay_AMT_SENS
    ,PR_Result_Worked_HRS_SENS
    ,PR_Worked_Total_HRS_SENS
    ,PR_Cycle_Type
    ,PR_OffCycle_Reason_SENS
    ,PR_Home_State
    ,PR_Work_State
    ,PR_Payment_Method
    ,PR_Check_NBR
    ,PR_Retro_Source
    ,PR_Payroll_Completed_DTM
    ,PR_Pay_Calculation_Status
    ,PR_Accounting_Status
    ,PR_Batch_ID
    ,PR_WID
    ,PR_Position
    ,PR_Last_Upd_DTM
    ,PR_Last_Functionally_Upd_DTM
    ,row_number() over(Partition by PR_WID order by PR_PAYMENT_OR_REVERSAL_DT desc ) as rn
from raw_data
qualify rn = 1
order by EMPloyee_id 
)
--intergrate with dimensions emplyoee and position
select 
    {{ dbt_utils.surrogate_key(['PR_WID'])}} as PAYROLL_RESULT_PK
    ,coalesce(emp.emp_sk,'-1') as EMP_SK
    ,coalesce(pos.POSITION_SK,'-1') as POSITION_SK
    ,Employee_ID as EMP_ID
    ,PR_Pay_Group_Detail as PR_PAY_GROUP_DTL
    ,PR_Payment_Type as PR_PYMT_TYPE
    ,PR_Journal_DT as PR_JOURNAL_DT
    ,PR_Federal_Marital_Status_SENS as PR_FEDERAL_MARITAL_STATUS_SENS
    ,PR_Federal_Allowance_CNT_SENS as PR_FEDERAL_ALLOWANCE_CNT_SENS
    ,PR_Federal_Allowance_Plus_SENS as PR_FEDERAL_ALLOWANCE_PLUS_SENS
    ,PR_State_Allowance_CNT_SENS as PR_STATE_ALLOWANCE_CNT_SENS
    ,PR_Period_Start_DT as PR_PERIOD_START_DT
    ,PR_GTN_Or_Period_End_DT as PR_GTN_OR_PERIOD_END_DT
    ,PR_Payment_or_Reversal_DT as PR_PYMT_OR_REVERSAL_DT
    ,PR_Gross_Pay_AMT_SENS as PR_GROSS_PAY_AMT_SENS
    ,PR_Net_Pay_AMT_SENS as PR_NET_PAY_AMT_SENS
    ,PR_Result_Worked_HRS_SENS as PR_RESULT_WORKED_HRS_SENS
    ,PR_Worked_Total_HRS_SENS as PR_WORKED_TOTAL_HRS_SENS
    ,PR_Cycle_Type as PR_CYCLE_TYPE
    ,PR_OffCycle_Reason_SENS as PR_OFFCYCLE_REASON_SENS
    ,PR_Home_State as PR_HOME_STATE
    ,PR_Work_State as PR_WORK_STATE
    ,PR_Payment_Method as PR_PYMT_METHOD
    ,PR_Check_NBR as PR_CHECK_NBR
    ,PR_Retro_Source as PR_RETRO_SOURCE
    ,PR_Payroll_Completed_DTM as PR_PAYROLL_COMPLETED_DTM
    ,PR_Pay_Calculation_Status as PR_PAY_CALC_STATUS
    ,PR_Accounting_Status as PR_ACCOUNTING_STATUS
    ,PR_Batch_ID as PR_BATCH_ID
    ,PR_WID as PR_WID
    ,PR_Position as PR_POSITION
    ,PR_Last_Upd_DTM as PR_LAST_UPD_DTM
    ,PR_Last_Functionally_Upd_DTM as PR_LAST_FUNCTIONALLY_UPD_DTM
    ,to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss')) as ins_batch_id
from final f
    left join {{ ref('DIM_EMPLOYEE') }} emp 
        on f.Employee_ID=emp.EMP_ID 
         and to_date(f.PR_PAYMENT_OR_REVERSAL_DT) between to_date(emp.eff_dt) and to_date(emp.disc_dt)
    left join POSITION pos 
        on  TRIM(SUBSTR(f.PR_Position,1,10)) = pos.POSITION_ID 
        and to_date(f.PR_PAYMENT_OR_REVERSAL_DT) between to_date(pos.eff_dt) and to_date(pos.disc_dt)
