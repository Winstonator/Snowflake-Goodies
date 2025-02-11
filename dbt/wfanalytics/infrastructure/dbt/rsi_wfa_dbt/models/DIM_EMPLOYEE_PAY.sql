{{
    config(
        materialized='incremental',
        unique_key ='EMP_PAY_SK'
    )
}}
--Captures Type 2 data 
with rawdata as (
    select * 
    from {{ source('STAGING','STG_WKD_EMPLOYEE_EXTRACT') }}
 --incremental logic to pull updates or new employees
     {% if is_incremental() %}
     where  
     RPT_EFFECTIVE_DT > ( select max(RPT_EFF_DT) from {{this}}  )
     {% endif %}
)
 --get all records for update employee from STG table
, reqData as (
        select * 
        from {{ source('STAGING','STG_WKD_EMPLOYEE_EXTRACT') }}  
        where EIN in (select EIN from rawdata )
)
, dedup as (
    select 
        EIN as EMP_ID,
        Total_FTE as TOTAL_FTE,
        FTE_Percent as FTE_PCT,
        Time_Type as TIME_TYPE,
        Default_Weekly_Hours as DEFAULT_WEEKLY_HRS,
        Time_Off_Service_DT as TIME_OFF_SERVICE_DT,
        Cost_To_Replace_SENS as COST_TO_REPLACE_SENS,
        Costing_Allocation_Exists_for_Worker as COSTING_ALLOCATION_EXISTS_FOR_WORKER,
        Pay_Group as PAY_GROUP,
        Pay_Rate_Type as PAY_RATE_TYPE,
        Pay_Types_Without_Payment_Election as PAY_TYPES_WITHOUT_PYMT_ELECTION,
        Total_Base_Pay_Payroll_SENS as TOTAL_BASE_PAY_PAYROLL_SENS,
        Total_Pay_Amt_SENS as TOTAL_PAY_AMT_SENS,
        Hourly_Rate_Amt_SENS as HRLY_RATE_AMT_SENS,
        RPT_EFFECTIVE_DT as RPT_EFF_DT,
        row_number() over(partition by EIN,	Total_FTE,	FTE_Percent,	Time_Type,	Default_Weekly_Hours,	Time_Off_Service_DT,	Cost_To_Replace_SENS,	Costing_Allocation_Exists_for_Worker,	Pay_Group,	Pay_Rate_Type,	Pay_Types_Without_Payment_Election,	Total_Base_Pay_Payroll_SENS, Total_Pay_Amt_SENS, Hourly_Rate_Amt_SENS order by RPT_EFFECTIVE_DT ) as RN 
    from reqData
        QUALIFY RN=1
)
    , version as (
        select *, row_number() over(partition by EMP_ID order by RPT_EFF_DT asc) as version_number 
        from dedup
        ) 
    , final as (
        select v1.*, v1.RPT_EFF_DT as EFF_DT, v2.RPT_EFF_DT -1  as DISC_DT
        from version v1 
        left join version v2 on v1.version_number +1 =v2.version_number and v1.EMP_ID = v2.EMP_ID 
        order by v1.version_number
        ) 
    select 
        {{ dbt_utils.surrogate_key(['EMP_ID','RPT_EFF_DT'])}} as EMP_PAY_SK,
        EMP_ID,	TOTAL_FTE,	FTE_PCT,	TIME_TYPE,	DEFAULT_WEEKLY_HRS,	TIME_OFF_SERVICE_DT,	COST_TO_REPLACE_SENS,	COSTING_ALLOCATION_EXISTS_FOR_WORKER,	PAY_GROUP,	PAY_RATE_TYPE,	PAY_TYPES_WITHOUT_PYMT_ELECTION,	TOTAL_BASE_PAY_PAYROLL_SENS,	TOTAL_PAY_AMT_SENS,	HRLY_RATE_AMT_SENS, RPT_EFF_DT,
        'WKD' AS REC_SRC,
        EFF_DT, 
        coalesce(DISC_DT, '9999-12-31') as DISC_DT, 
        CASE WHEN DISC_DT is null then TRUE else FALSE END as IS_CURRENT,   
        (select to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss'))) as INS_BATCH_ID,
        TO_NUMBER(to_varchar(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID,
        {{ dbt_utils.surrogate_key(['EMP_ID',	'TOTAL_FTE',	'FTE_PCT',	'TIME_TYPE',	'DEFAULT_WEEKLY_HRS',	'TIME_OFF_SERVICE_DT',	'COST_TO_REPLACE_SENS',	'COSTING_ALLOCATION_EXISTS_FOR_WORKER',	'PAY_GROUP',	'PAY_RATE_TYPE',	'PAY_TYPES_WITHOUT_PYMT_ELECTION',	'TOTAL_BASE_PAY_PAYROLL_SENS',	'TOTAL_PAY_AMT_SENS',	'HRLY_RATE_AMT_SENS'])}} as DBT_HASH

    from final
