{{
    config(
        materialized='table',
        unique_key ='SUBTRACT_FROM_SK',
        full_refresh = True
        )
}}


WITH
STG_PAY_ACC AS (
    SELECT 
    PAY_ACCUMULATION_WID,
    PAY_ACCUMULATION_CD,
    'WKD' AS REC_SRC, 
    A.VALUE::STRING AS SUBTRACT_FROM_PAY_ACCUMULATION
    FROM {{ source('STAGING','STG_WKD_PAY_ACCUMULATORS') }},
    LATERAL FLATTEN(INPUT => SPLIT(SUBTRACT_FROM_PAY_ACCUMULATION, ';')) A
    QUALIFY ROW_NUMBER() OVER(PARTITION BY PAY_ACCUMULATION_WID, SUBTRACT_FROM_PAY_ACCUMULATION ORDER BY SF_INSERT_TIMESTAMP DESC) = 1
),

DIM_PAY_EARN AS (
    SELECT
    CALC_CD,
    CASE 
        WHEN CALC_CD IS NOT NULL THEN 1 
        ELSE 0 
    END AS EARNING_CD_IND
    FROM {{ ref('DIM_PAYCODES') }}
    WHERE PAY_CD_TYPE IN ('Workday_Earning_Code','Earning_Code')
),

DIM_PAY_DEDUCT AS (
    SELECT
    CALC_CD,
    CASE 
        WHEN CALC_CD IS NOT NULL THEN 1 
        ELSE 0 
    END AS DEDUCTION_CD_IND
    FROM {{ ref('DIM_PAYCODES') }}
    WHERE PAY_CD_TYPE IN ('Workday_Deduction_Code','Deduction_Code')
),

DIM_PAY_COMP AS (
    SELECT
    PAY_COMPONENT_GROUP_NAME,
    CASE 
        WHEN PAY_COMPONENT_GROUP_NAME IS NOT NULL THEN 1 
        ELSE 0 
    END AS PAY_COMPONENT_GROUP_IND
    FROM {{ ref('DIM_PAY_COMPONENT_GROUPS') }}
),

DIM_PAY_CALCS AS (
    SELECT
    PAYROLL_CALC,
    CASE 
        WHEN PAYROLL_CALC IS NOT NULL THEN 1 
        ELSE 0 
    END AS PCRC_IND
    FROM {{ ref('DIM_PAY_COMPONENTS_REL_CALCS') }}
)

SELECT 
    {{ dbt_utils.surrogate_key(['PAY_ACCUMULATION_WID','SUBTRACT_FROM_PAY_ACCUMULATION'])}} AS SUBTRACT_FROM_SK,
    PAY_ACCUMULATION_WID || '-' || SUBTRACT_FROM_PAY_ACCUMULATION AS SUBTRACT_FROM_ID,
    PAY_ACCUMULATION_WID,
    PAY_ACCUMULATION_CD,
    TRIM(SUBTRACT_FROM_PAY_ACCUMULATION) AS SUBTRACT_FROM_PAY_ACCUMULATION,
    DIM_PAY_EARN.EARNING_CD_IND,
    DIM_PAY_DEDUCT.DEDUCTION_CD_IND,
    DIM_PAY_COMP.PAY_COMPONENT_GROUP_IND,
    DIM_PAY_CALCS.PCRC_IND,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as INS_BATCH_ID,
    TO_NUMBER(to_varchar(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID,
    REC_SRC
FROM STG_PAY_ACC

LEFT JOIN DIM_PAY_EARN
    ON TRIM(STG_PAY_ACC.SUBTRACT_FROM_PAY_ACCUMULATION) = TRIM(DIM_PAY_EARN.CALC_CD)
LEFT JOIN DIM_PAY_DEDUCT
    ON TRIM(STG_PAY_ACC.SUBTRACT_FROM_PAY_ACCUMULATION) = TRIM(DIM_PAY_DEDUCT.CALC_CD)
LEFT JOIN DIM_PAY_COMP 
    ON TRIM(STG_PAY_ACC.SUBTRACT_FROM_PAY_ACCUMULATION) = TRIM(DIM_PAY_COMP.PAY_COMPONENT_GROUP_NAME)
LEFT JOIN DIM_PAY_CALCS
    ON TRIM(STG_PAY_ACC.SUBTRACT_FROM_PAY_ACCUMULATION) = TRIM(DIM_PAY_CALCS.PAYROLL_CALC)


