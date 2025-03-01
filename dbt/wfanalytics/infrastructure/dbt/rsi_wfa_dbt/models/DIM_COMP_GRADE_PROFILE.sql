{{
    config(
        materialized='incremental',
        unique_key ='COMP_GRADE_PROFILE_SK',
        full_refresh=false,
        post_hook='{{ update_dim_comp_grade_profile_disappear() }}'
    )
}}

WITH RAWDATA AS (
    SELECT *
    FROM {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }} 
    WHERE 1=1
    {% if is_incremental() %}
    AND RPT_EFFECTIVE_DT > ( SELECT MAX(RPT_EFF_DT) FROM {{this}}  )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY COMP_GRADE_PROFILE_WID ORDER BY RPT_EFFECTIVE_DT desc) = 1
),

REQDATA AS (
    SELECT * 
    FROM {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }} 
    WHERE COMP_GRADE_PROFILE_WID IN (SELECT COMP_GRADE_PROFILE_WID FROM RAWDATA )
),

COMP AS (
    SELECT
    COMP_GRADE_WID AS COMP_GRADE_WID,
    COMP_GRADE_PROFILE_WID AS COMP_GRADE_PROFILE_WID,
    GRADE_PROFILE_REF_ID AS GRADE_PROFILE_ID,
    GRADE_PROFILE_FREQUENCY_ID AS FREQ_ID,
    COMPENSATION_GRADE_PROFILE_ELIGIBILITY_RULE_REFID AS ELIGIBILITY_RULE_ID,
    COMPENSATION_GRADE AS COMP_GRADE,
    COMPENSATION_GRADE_PROFILE AS COMP_GRADE_PROFILE,
    COMPENSATION_BASIS AS COMP_BASIS,
    COMPENSATION_GRADE_PROFILE_ELIGIBILITY_RULE_NAME AS ELIGIBILITY_RULE_NAME,
    INACTIVE AS INACTIVE,
    NUMBER_OF_SEGMENTS AS NBR_SEGMENTS,
    GRADE_PROFILE_MINIMUM_SENS AS GRADE_PROFILE_MIN_SENS,
    GRADE_PROFILE_MIDPOINT_SENS AS GRADE_PROFILE_MID_SENS,
    GRADE_PROFILE_MAXIMUM_SENS AS GRADE_PROFILE_MAX_SENS,
    PAY_RANGE_SPREAD_SENS AS PAY_RANGE_SPREAD_SENS,
    RPT_EFFECTIVE_DT AS RPT_EFF_DT,
    ROW_NUMBER() OVER (PARTITION BY COMP_GRADE_WID ORDER BY RPT_EFFECTIVE_DT) AS RN,
    ROW_NUMBER() OVER (PARTITION BY
        COMP_GRADE_WID,
        COMP_GRADE_PROFILE_WID,
        GRADE_PROFILE_REF_ID,
        GRADE_PROFILE_FREQUENCY_ID,
        COMPENSATION_GRADE_PROFILE_ELIGIBILITY_RULE_REFID,
        COMPENSATION_GRADE,
        COMPENSATION_GRADE_PROFILE,
        COMPENSATION_BASIS,
        COMPENSATION_GRADE_PROFILE_ELIGIBILITY_RULE_NAME,
        INACTIVE,
        NUMBER_OF_SEGMENTS,
        GRADE_PROFILE_MINIMUM_SENS,
        GRADE_PROFILE_MIDPOINT_SENS,
        GRADE_PROFILE_MAXIMUM_SENS,
        PAY_RANGE_SPREAD_SENS
        ORDER BY RPT_EFFECTIVE_DT) AS RNO
FROM {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }}
),
DE_DUP AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY
        COMP_GRADE_WID,
        COMP_GRADE_PROFILE_WID,
        GRADE_PROFILE_ID,
        FREQ_ID,
        ELIGIBILITY_RULE_ID,
        COMP_GRADE,
        COMP_GRADE_PROFILE,
        COMP_BASIS,
        ELIGIBILITY_RULE_NAME,
        INACTIVE,
        NBR_SEGMENTS,
        GRADE_PROFILE_MIN_SENS,
        GRADE_PROFILE_MID_SENS,
        GRADE_PROFILE_MAX_SENS,
        PAY_RANGE_SPREAD_SENS,
        RN - RNO
        ORDER BY RPT_EFF_DT) AS NUM
    FROM COMP
    QUALIFY NUM = 1
),

VERSION AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY COALESCE(COMP_GRADE_PROFILE_WID,'') ORDER BY RPT_EFF_DT DESC)
    AS VERSION_NUMBER FROM DE_DUP
),

FINAL AS (
    SELECT V1.*, V1.RPT_EFF_DT AS EFF_DT, V2.RPT_EFF_DT -1 AS DISC_DT
    FROM VERSION V1 
    LEFT JOIN VERSION V2 ON V1.VERSION_NUMBER - 1 = V2.VERSION_NUMBER AND V1.COMP_GRADE_PROFILE_WID = V2.COMP_GRADE_PROFILE_WID 
    ORDER BY V1.VERSION_NUMBER
)

SELECT 
    {{ dbt_utils.surrogate_key(['COMP_GRADE_PROFILE_WID','RPT_EFF_DT']) }} AS COMP_GRADE_PROFILE_SK,
    COMP_GRADE_WID,
    COMP_GRADE_PROFILE_WID,
    GRADE_PROFILE_ID,
    FREQ_ID,
    ELIGIBILITY_RULE_ID,
    COMP_GRADE,
    COMP_GRADE_PROFILE,
    COMP_BASIS,
    ELIGIBILITY_RULE_NAME,
    INACTIVE,
    NBR_SEGMENTS,
    GRADE_PROFILE_MIN_SENS,
    GRADE_PROFILE_MID_SENS,
    GRADE_PROFILE_MAX_SENS,
    PAY_RANGE_SPREAD_SENS,
    RPT_EFF_DT,
    'WKD' AS REC_SRC,
    EFF_DT,
    COALESCE(DISC_DT, '9999-12-31') as DISC_DT,
    CASE WHEN DISC_DT IS NULL THEN TRUE ELSE FALSE END AS IS_CURRENT,         
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS INS_BATCH_ID,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID,
    {{ dbt_utils.surrogate_key([
        'COMP_GRADE_WID',
        'COMP_GRADE_PROFILE_WID',
        'GRADE_PROFILE_ID',
        'FREQ_ID',
        'ELIGIBILITY_RULE_ID',
        'COMP_GRADE',
        'COMP_GRADE_PROFILE',
        'COMP_BASIS',
        'ELIGIBILITY_RULE_NAME',
        'INACTIVE',
        'NBR_SEGMENTS',
        'GRADE_PROFILE_MIN_SENS',
        'GRADE_PROFILE_MID_SENS',
        'GRADE_PROFILE_MAX_SENS',
        'PAY_RANGE_SPREAD_SENS']) }} AS DBT_HASH
FROM FINAL
