{{
    config(
        materialized='incremental',
        unique_key ='AGENCY_SK'
    )
}}

WITH
STG_AGENCY AS ( select * , 'WKD' AS REC_SOURCE
    from {{ source('STAGING','STG_WKD_RECRUITING_AGENCY') }} 
    
    {% if is_incremental() %}
     WHERE 
    RPT_EFFECTIVE_DT > ( select max(RPT_EFF_DT) from {{this}}  )
    {% endif %}
    
),
INS_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS INS_BATCH_ID, 1 AS BATCH_KEY_ID
),

AGENCY AS(
    SELECT 1 AS BATCH_KEY_ID,
    RECRUITING_AGENCY_REFID AS AGENCY_REF_ID,
    RECRUITING_AGENCY_NAME AS AGENCY_NAME,
	RECRUITING_AGENCY_DESCRIPTION AS AGENCY_DESC,
	TO_NUMBER(RECRUITING_AGENCY_ACTIVE_IND) AS ACTIVE_IND,
	RECRUITING_AGENCY_JOB_POSTING_SITE AS JOB_POSTING_SITE,
	RECRUITING_AGENCY_OWNERSHIP_MONTHS AS OWNERSHIP_MONTHS,
	RECRUITING_SOURCE AS AGENCY_SRC,
	RECRUITING_AGENCY_TYPE AS AGENCY_TYPE,
	RECRUITING_AGENCY_FEE_TYPE AS FEE_TYPE,
	TO_NUMBER(RECRUITING_AGENCY_FEE_AMT) AS FEE_AMT,
	RECRUITING_AGENCY_WID AS AGENCY_WID,
	RECRUITING_AGENCY_CREATED_DTM AS CREATED_DTM,
    RECRUITING_AGENCY_LAST_UPD_DTM AS LAST_UPD_DTM,
    RPT_EFFECTIVE_DT AS RPT_EFF_DT,
    REC_SOURCE AS REC_SRC,
    ROW_NUMBER() OVER(PARTITION BY AGENCY_REF_ID  ORDER BY RPT_EFFECTIVE_DT desc) RN, 
    TO_NUMBER(to_varchar(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID
FROM STG_AGENCY
)

SELECT
    {{ dbt_utils.surrogate_key(['AGENCY_REF_ID']) }} AS AGENCY_SK,
    AGENCY_REF_ID,
    AGENCY_NAME,
    AGENCY_DESC,
    ACTIVE_IND,
    JOB_POSTING_SITE,
    OWNERSHIP_MONTHS,
    AGENCY_SRC,
    AGENCY_TYPE,
    FEE_TYPE,
    FEE_AMT,
    AGENCY_WID,
    CREATED_DTM,
    LAST_UPD_DTM,
    RPT_EFF_DT,
    REC_SRC,
    INS_BATCH_ID as INS_BATCH_ID,
    UPD_BATCH_ID
FROM AGENCY
LEFT JOIN INS_BATCH_ID USING (BATCH_KEY_ID)
WHERE RN=1