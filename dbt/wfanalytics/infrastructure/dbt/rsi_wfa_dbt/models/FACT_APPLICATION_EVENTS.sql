{{
    config(
        materialized='incremental',
        unique_key='JOB_APP_EVENT_PK',
        full_refresh=false
        )
}}

WITH
STG_WKD_JOB_APPLICATION_EVENT AS (
    SELECT *, 'WKD' as REC_SOURCE, 1 AS BATCH_KEY_ID FROM {{ source('STAGING','STG_WKD_JOB_APPLICATION_EVENT') }}
),

APPLICATION AS (
    SELECT OVERALL_BP_WID, CAND_APP_SK FROM {{ ref('DIM_APPLICATION') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['BP_EVENT_COMPLETED_WID'])}} AS JOB_APP_EVENT_PK,
    COALESCE(APP.CAND_APP_SK, '-1') AS CAND_APP_SK,
    STG.OVERALL_BP_WID as OVERALL_BP_WID,
    STG.BP_EVENT_COMPLETED_WID as BP_EVENT_COMPLETED_WID,
    STG.OVERALL_BP as OVERALL_BP,
    STG.OVERALL_BP_INITIATED_DTM as OVERALL_BP_INITIATED_DTM,
    STG.BP_EVENT_DEFINITION as BP_EVENT_DEF,
    STG.BP_EVENT_INITIATED_DTM as BP_EVENT_INITIATED_DTM,
    STG.BP_EVENT_COMPLETED_DTM as BP_EVENT_COMPLETED_DTM,
    STG.BP_EVENT_RECORD as BP_EVENT_REC,
    STG.BP_STEP_TYPE as BP_STEP_TYPE,
    STG.BP_STEP_DESCRIPTION as BP_STEP_DESC,
    STG.BP_STEP_STATUS as BP_STEP_STATUS,
    STG.BP_STEP_ORDER as BP_STEP_ORDER,
    STG.BP_STEP_COMPLETED_BY_NAME as BP_STEP_COMPLETED_BY_NAME,
    STG.BP_STEP_COMPLETED_BY_PERSON as BP_STEP_COMPLETED_BY_PERSON,
    STG.BP_STEP_COMMENTS as BP_STEP_COMMENTS,
    STG.OVERALL_BP_TYPE as OVERALL_BP_TYPE,
    STG.OVERALL_BP_COMPLETED_DTM as OVERALL_BP_COMPLETED_DTM,
    STG.OVERALL_BP_COMPLETED_DT as OVERALL_BP_COMPLETED_DT,
    STG.BP_TRANSACTION_STATUS as BP_TRANSACTION_STATUS,
    STG.REC_SOURCE as REC_SRC,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS INS_BATCH_ID,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID
FROM STG_WKD_JOB_APPLICATION_EVENT STG
LEFT JOIN APPLICATION APP ON APP.OVERALL_BP_WID = STG.OVERALL_BP_WID
{% if is_incremental() %}
    WHERE (STG.BP_EVENT_COMPLETED_DTM >= '{{ get_max_event_time('BP_EVENT_COMPLETED_DTM') }}'
        OR 
    JOB_APP_EVENT_PK IN (
        SELECT JOB_APP_EVENT_PK FROM {{ this }}
        WHERE CAND_APP_SK='-1' )
    )
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY BP_EVENT_COMPLETED_WID ORDER BY BP_EVENT_COMPLETED_DTM DESC) = 1
