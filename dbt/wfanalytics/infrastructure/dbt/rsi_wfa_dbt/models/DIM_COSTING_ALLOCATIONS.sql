{{
    config(
        materialized='table'
    )
}}

with rawdata as (
    select *
    from {{ source('STAGING','STG_WKD_COSTING_ALLOCATIONS') }} 
),
dedup as (
    select * 
    from rawdata
    qualify row_number() over(partition by Employee_ID, Company_ID, Cost_Center_ID, Override_Type, Override_Start_DT, Override_End_DT, Pay_Code_SENS order by RPT_RUN_DTM desc) = 1
)
--> aggregation calculation and logic re-calculate sum when same data get updates..
, final as (
select 
    Employee_ID	as EMP_ID,
    Company_ID as COMPANY_ID,
    Cost_Center_ID as COST_CENTER_ID,
    Override_Type as OVERRIDE_TYPE,
    Override_Start_DT as OVERRIDE_START_DT,
    Override_End_DT as OVERRIDE_END_DT,
    Pay_Code_SENS as PAY_CD_SENS,
    cast(sum(Distribution_PCT_SENS) as varchar) as DIST_PCT_SENS
from rawdata
group by 1,2,3,4,5,6,7
)
select     
{{ dbt_utils.surrogate_key(['EMP_ID','COMPANY_ID','COST_CENTER_ID','OVERRIDE_TYPE','OVERRIDE_START_DT','OVERRIDE_END_DT','PAY_CD_SENS']) }} as COSTING_ALLOCATIONS_SK,
    EMP_ID,
    COMPANY_ID,
    COST_CENTER_ID,
    OVERRIDE_TYPE,
    OVERRIDE_START_DT,
    OVERRIDE_END_DT,
    PAY_CD_SENS,
    DIST_PCT_SENS
from final
