{{
    config(
        materialized='table',
        tags=["core","dim","scheduled-daily"],
        full_refresh=True,
        post_hook=[
            "UPDATE {{ this }} d1
            SET d1.disc_dt=d2.disc_dt_2,d1.next_value=d2.next_value_2
            FROM
            (
            select *,dateadd(day,-1,lead(eff_dt) over(partition by POSITION_ID, POSITION_MGMT_WID, change_column order by eff_dt)) as disc_dt_2,
            lead(current_value) over(partition by POSITION_ID, POSITION_MGMT_WID, change_column order by eff_dt) as next_value_2
            from {{ this }} ) d2 where d1.POSITION_ID=d2.POSITION_ID and d1.POSITION_MGMT_WID = d2.POSITION_MGMT_WID 
            and d1.change_column=d2.change_column and d1.eff_dt=d2.eff_dt",

            "UPDATE {{ this }} set disc_dt='9999-12-31',is_current=1 where disc_dt is null",
            "UPDATE {{ this }} set is_current=0 where is_current is null",

            "UPDATE {{ this }} d1
            SET d1.is_deleted=1,d1.deleted_dt=dateadd(day,1,d2.RPT_EFFECTIVE_DT),d1.CHANGE_TYPE='Position_Deleted'
            FROM
            (
            select POSITION_ID, POSITION_MANAGEMENT_WID, RPT_EFFECTIVE_DT from 
            (select POSITION_ID, POSITION_MANAGEMENT_WID, RPT_EFFECTIVE_DT,
            lead(RPT_EFFECTIVE_DT) OVER (partition by POSITION_ID, POSITION_MANAGEMENT_WID ORDER BY RPT_EFFECTIVE_DT) next_RPT_EFFECTIVE_DT 
            from {{ source('STAGING','STG_WKD_POSITION_INTERIM') }} ) emp where next_RPT_EFFECTIVE_DT is null 
            and RPT_EFFECTIVE_DT <> (select max(RPT_EFFECTIVE_DT) from  {{ source('STAGING','STG_WKD_POSITION_INTERIM') }} )
            ) d2 where d1.POSITION_ID=d2.POSITION_ID and d1.POSITION_MGMT_WID = d2.POSITION_MANAGEMENT_WID and d1.is_current=1",

            "UPDATE {{ this }} SET CHANGE_TYPE='Rescind' WHERE eff_dt > disc_dt and change_type is null"

        ]
    )
}}

with psn as (
SELECT 
distinct 
    POSITION_ID,
    POSITION_MGMT_WID, --(wid is neeeded because it covers when employees are overlaps at same time period)
    Change_column, 
    current_value,
    RPT_EFF_DT
FROM (

        SELECT 
        POSITION_ID,
        POSITION_MANAGEMENT_WID as POSITION_MGMT_WID,
        STAFFING_STATUS,
        POSITION_OPEN_REASON,
        WORKERS_COMPENSATION_CD as WORKERS_COMP_CD,
        AVAILABLE_FOR_OVERLAP_IND,
        AVAILABLE_FOR_RECRUITING_IND,
        COST_CENTER,
        DYNAMIC_MANAGEMENT_LEVEL as DYNAMIC_MGMT_LEVEL,
        JOB_CODE as JOB_CD,
        EIN as EMP_ID,
        ORG_ID as SUP_ORG_ID,
        MANAGER_ID as MGR_ID,
        AVAILABLE_FOR_HIRE_IND,
        CAST(FREEZE_DT AS VARCHAR) FREEZE_DT,
        FREEZE_REASON,
        FROZEN,
        POSITION_COMPENSATION_GRADE_PROFILE as POSITION_COMP_GRADE_PROFILE,
        RPT_EFFECTIVE_DT as RPT_EFF_DT
        FROM {{ ref('STG_WKD_POSITION_INTERIM') }}
        qualify row_number() over(partition by POSITION_ID, POSITION_MGMT_WID, RPT_EFFECTIVE_DT order by RPT_EFFECTIVE_DT desc, STAFFING_STATUS desc)= 1
        order by RPT_EFFECTIVE_DT desc
) A 
UNPIVOT( 
      current_value FOR Change_column IN (
            STAFFING_STATUS,
            POSITION_OPEN_REASON,
            WORKERS_COMP_CD,
            AVAILABLE_FOR_OVERLAP_IND,
            AVAILABLE_FOR_RECRUITING_IND,
            COST_CENTER,
            DYNAMIC_MGMT_LEVEL,
            JOB_CD,
            EMP_ID,
            SUP_ORG_ID,
            MGR_ID,
            AVAILABLE_FOR_HIRE_IND,
            FREEZE_DT,
            FREEZE_REASON,
            FROZEN,
            POSITION_COMP_GRADE_PROFILE
      )
)
),
psn_unpivot as (
  select *, LAG(current_value) OVER (partition by POSITION_ID, POSITION_MGMT_WID, Change_column ORDER BY RPT_EFF_DT) prior_value 
  from psn
)
, final as (
SELECT
  Change_column, 
  POSITION_ID, 
  POSITION_MGMT_WID,
  CURRENT_VALUE, 
  RPT_EFF_DT, 
  PRIOR_VALUE 
from 
  psn_unpivot 
where 
  (current_value <> prior_value or prior_value is null) 
order by 2, 1, RPT_EFF_DT
)
select 
  {{ dbt_utils.surrogate_key(['Change_Column','POSITION_ID', 'POSITION_MGMT_WID','RPT_EFF_DT']) }} AS POSITION_EVENT_SK,
  Change_Column,
  POSITION_ID,
  POSITION_MGMT_WID,
  Current_Value,
  Prior_Value,
  NULL::VARCHAR(200) AS Next_Value,
  RPT_EFF_DT AS Eff_Dt,
  NULL::DATE AS Disc_Dt,
  NULL::smallint as Is_Current,
  NULL::smallint as Is_Deleted,
  NULL::DATE AS Deleted_Dt,
  NULL::VARCHAR(200) AS Change_Type
from final
