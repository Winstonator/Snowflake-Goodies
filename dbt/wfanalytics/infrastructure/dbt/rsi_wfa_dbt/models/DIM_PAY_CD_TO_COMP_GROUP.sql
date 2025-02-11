{{
    config(
        materialized='table',
        unique_key ='PAYCODE_COMPONENT_GROUP_SK',
        full_refresh = True
        )
}}

select 
{{ dbt_utils.surrogate_key(['PCE.Pay_Component_Group_ID','PCE.Paycode_Earning_ID','PCD.Paycode_Deduction_ID']) }} AS PAYCODE_COMPONENT_GROUP_SK,
PCE.Pay_Component_Group_ID AS PAY_COMPONENT_GROUP_ID,
PCE.Paycode_Earning_ID AS PAY_CD_EARNING_ID,
PCD.Paycode_Deduction_ID AS	PAY_CD_DEDUCTION_ID
from {{ source('STAGING','STG_WKD_PAY_COMP_EARNING') }} PCE
full outer join {{ source('STAGING','STG_WKD_PAY_COMP_DEDUCTION') }} PCD
on PCE.PAY_COMPONENT_GROUP_ID = PCD.PAY_COMPONENT_GROUP_ID
qualify ROW_NUMBER() OVER(PARTITION BY PCE.Pay_Component_Group_ID, PCE.Paycode_Earning_ID, PCD.Paycode_Deduction_ID ORDER BY PCE.RPT_RUN_DTM DESC) = 1
