{{
    config(
        materialized='incremental',
        unique_key='POSITION_CHANGE_PK'
        )
}}

WITH
WORKER_POSITION_CHANGES AS (
    SELECT *
    , 'WKD' AS REC_SRC
    , TRIM(SUBSTRING(NEW_POSITION,1,CHARINDEX(' ',TRIM(NEW_POSITION) ) )) AS NEW_POSITION_ID
    , TRIM(SUBSTRING(POSITION_CURRENT,1,CHARINDEX(' ',TRIM(POSITION_CURRENT) ) )) AS PREV_POSITION_ID
    , ('CC_' || TRIM(SUBSTRING(PREVIOUS_COST_CENTER,1,4)) || TRIM(SUBSTRING(PREVIOUS_COST_CENTER,8,4)) 
		|| TRIM(SUBSTRING(PREVIOUS_COST_CENTER,15,4))) AS PREV_CC_CODE
    , ('CC_' || TRIM(SUBSTRING(NEW_COST_CENTER,1,4)) || TRIM(SUBSTRING(NEW_COST_CENTER,8,4)) 
		|| TRIM(SUBSTRING(NEW_COST_CENTER,15,4))) AS NEW_CC_CODE
	, ('LOC_' || SUBSTR(strtok(NEW_LOCATION, '(', 2),1,6)) as NEW_LOC_ID
	, ('LOC_' || SUBSTR(strtok(PREVIOUS_LOCATION, '(', 2),1,6)) as PREV_LOC_ID
	, split_part(PREVIOUS_UNION, ' -', 0) as PREV_UNION_ID
	, split_part(NEW_UNION, ' -',0) as NEW_UNION_ID
    FROM {{ source('STAGING','STG_WKD_WORKER_POSITION_CHANGES') }} 
),

CTE_POSITION AS (
    SELECT POSITION_ID
    , POSITION_SK
    , EFF_DT
    , DISC_DT
    FROM {{ ref('DIM_POSITION') }}
),

EMPLOYEE AS (
    SELECT EMP_ID AS EMPID
    , EMP_SK
    , EFF_DT
    , DISC_DT 
    FROM {{ ref('DIM_EMPLOYEE') }}
),

CTE_JOB AS (
    SELECT JOB_CD
    , JOB_SK 
    FROM {{ ref('DIM_JOB') }}
),

CTE_ORG AS (
    SELECT COST_CENTER_CD 
    , ORG_SK 
    , RPT_EFF_DT
    , DISC_DT
    FROM  {{ ref('DIM_ORGANIZATION') }}
    WHERE COST_CENTER_CD is not null
),

CTE_LOC AS (
    SELECT LOCATION_SK 
    , LOCATION_ID 
    , EFF_DT
    , DISC_DT
    from {{ ref('DIM_LOCATION') }}
),

CTE_UNION AS (
    SELECT UNION_SK
    , UNION_ID
    , split_part(UNION_NAME, ' -', 0) as UNION_NAME
    FROM {{ ref('DIM_UNION') }}
)

SELECT 
POSITION_CHANGE_PK,
EMP_SK,
PREV_POSITION_SK,
NEW_POSITION_SK,
PREV_JOB_CD_SK,
NEW_JOB_CD_SK,
PREV_ORG_SK,
NEW_ORG_SK,
PREV_LOCATION_SK,
NEW_LOCATION_SK,
PREV_UNION_SK,
NEW_UNION_SK,
EMP_ID,
POSITION_ID,
WORKER_WID,
WORKER_EVENT_WID,
PARENT_BP,
BP_TYPE,
CHANGE_ACTION,
REASON_CD,
REASON,
STATUS,
APPROVED_BY_WORKERS,
INITIATED_DT,
COMPLETED_DT,
EFF_DT,
PREV_BASE_PAY_SENS,
NEW_BASE_PAY_SENS,
REC_SRC,
INS_BATCH_ID
FROM  (
SELECT 
    {{ dbt_utils.surrogate_key(['WORKER_EVENT_WID'])}} AS POSITION_CHANGE_PK, 
    COALESCE(EMPLOYEE.EMP_SK, '-1') AS EMP_SK,
    COALESCE(PREV_POSITION.POSITION_SK, '-1') AS PREV_POSITION_SK, 
    COALESCE(NEW_POSITION.POSITION_SK, '-1') AS NEW_POSITION_SK,
    COALESCE(PREV_JOB.JOB_SK, '-1') AS PREV_JOB_CD_SK, 
    COALESCE(NEW_JOB.JOB_SK, '-1') AS NEW_JOB_CD_SK,
    COALESCE(PREV_ORG.ORG_SK, '-1') AS PREV_ORG_SK, 
    COALESCE(NEW_ORG.ORG_SK, '-1') AS NEW_ORG_SK,
    COALESCE(PREV_LOC.LOCATION_SK, '-1') AS PREV_LOCATION_SK,
    COALESCE(NEW_LOC.LOCATION_SK, '-1') AS NEW_LOCATION_SK,  
    COALESCE(PREV_UNION.UNION_SK, '-1') AS PREV_UNION_SK,
    COALESCE(NEW_UNION.UNION_SK, '-1') AS NEW_UNION_SK,    
    EIN AS EMP_ID,
    WORKER_POSITION_CHANGES.POSITION_ID AS POSITION_ID,
    WORKER_WID AS WORKER_WID,
    WORKER_EVENT_WID AS WORKER_EVENT_WID,
    PARENT_BP AS PARENT_BP,
    BP_TYPE AS BP_TYPE,
    CHANGE_ACTION AS CHANGE_ACTION,
    REASON_CD AS REASON_CD,
    REASON AS REASON,
    STATUS AS STATUS,
    APPROVED_BY_WORKERS AS APPROVED_BY_WORKERS,
    INITIATED_DT AS INITIATED_DT,
    COMPLETED_DT AS COMPLETED_DT,
    EFFECTIVE_DT AS EFF_DT,
    PREVIOUS_BASE_PAY_SENS AS PREV_BASE_PAY_SENS,
    NEW_BASE_PAY_SENS AS NEW_BASE_PAY_SENS,
    REC_SRC AS REC_SRC,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as INS_BATCH_ID,
    ROW_NUMBER() OVER(PARTITION BY WORKER_EVENT_WID ORDER BY COMPLETED_DT DESC) RN
FROM WORKER_POSITION_CHANGES
LEFT JOIN CTE_POSITION  as PREV_POSITION
    ON WORKER_POSITION_CHANGES.PREV_POSITION_ID = PREV_POSITION.POSITION_ID  
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(PREV_POSITION.EFF_DT) AND TO_DATE(PREV_POSITION.DISC_DT)
LEFT JOIN CTE_POSITION AS  NEW_POSITION 
    ON WORKER_POSITION_CHANGES.NEW_POSITION_ID = NEW_POSITION.POSITION_ID 
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(NEW_POSITION.EFF_DT) AND TO_DATE(NEW_POSITION.DISC_DT)
LEFT JOIN EMPLOYEE
    ON WORKER_POSITION_CHANGES.EIN = EMPLOYEE.EMPID
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(EMPLOYEE.EFF_DT) AND TO_DATE(EMPLOYEE.DISC_DT)
LEFT JOIN CTE_JOB AS PREV_JOB
    ON WORKER_POSITION_CHANGES.PREVIOUS_JOB_CODE = PREV_JOB.JOB_CD
LEFT JOIN CTE_JOB AS NEW_JOB
    ON WORKER_POSITION_CHANGES.NEW_JOB_CODE = NEW_JOB.JOB_CD
LEFT JOIN CTE_ORG PREV_ORG
    on WORKER_POSITION_CHANGES.PREV_CC_CODE = PREV_ORG.COST_CENTER_CD
    and WORKER_POSITION_CHANGES.completed_dt between PREV_ORG.rpt_eff_dt and PREV_ORG.disc_dt
LEFT JOIN CTE_ORG AS NEW_ORG
    ON WORKER_POSITION_CHANGES.NEW_CC_CODE = NEW_ORG.COST_CENTER_CD
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(NEW_ORG.RPT_EFF_DT) AND TO_DATE(NEW_ORG.DISC_DT)
LEFT JOIN CTE_LOC AS PREV_LOC
    ON WORKER_POSITION_CHANGES.PREV_LOC_ID = PREV_LOC.LOCATION_ID
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(PREV_LOC.EFF_DT) AND TO_DATE(PREV_LOC.DISC_DT)
LEFT JOIN CTE_LOC AS NEW_LOC
    ON WORKER_POSITION_CHANGES.NEW_LOC_ID = NEW_LOC.LOCATION_ID
    AND TO_DATE(WORKER_POSITION_CHANGES.COMPLETED_DT) BETWEEN TO_DATE(NEW_LOC.EFF_DT) AND TO_DATE(NEW_LOC.DISC_DT)
LEFT JOIN CTE_UNION AS PREV_UNION
    ON WORKER_POSITION_CHANGES.PREV_UNION_ID = PREV_UNION.UNION_NAME
LEFT JOIN CTE_UNION AS NEW_UNION
    ON WORKER_POSITION_CHANGES.NEW_UNION_ID = NEW_UNION.UNION_NAME
{% if is_incremental() %}
    WHERE (WORKER_POSITION_CHANGES.COMPLETED_DT >= '{{ get_max_event_time('COMPLETED_DT') }}'
        OR 
    {{ dbt_utils.surrogate_key(['WORKER_EVENT_WID'])}} IN (
        SELECT POSITION_CHANGE_PK FROM {{ this }}
        WHERE EMP_SK = '-1' OR PREV_POSITION_SK = '-1' OR NEW_POSITION_SK = '-1'
        OR PREV_JOB_CD_SK = '-1' OR NEW_JOB_CD_SK = '-1'
        )
    )
{% endif %}
)WHERE RN=1
