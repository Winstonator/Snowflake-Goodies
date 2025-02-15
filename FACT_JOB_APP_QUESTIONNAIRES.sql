{{
    config(
        materialized='incremental',
        unique_key='JOB_APP_QSTR_PK'
        )
}}

WITH
JOB_APP_QUESTIONNAIRE AS (
    SELECT *, 'WKD' AS REC_SRC,  1 AS BATCH_KEY_ID FROM {{ source('STAGING','STG_WKD_JOB_APPLICATION_QUESTIONNAIRE') }} 
),

CANDIDATE AS (
    SELECT 
    JOB_APP_ID AS JOBAPPID, CAND_APP_SK FROM {{ ref('DIM_CANDIDATE') }}
)

SELECT 
JOB_APP_QSTR_PK,
CAND_APP_SK,
JOB_APP_ID,
MASKED_JOB_APP_ID,
CAND_ID,
JOB_APP_WID,
QSTR_ANSWER_WID,
JOB_APP,
JOB_REQ,
QR_CONTEXT,
RESPONDENT,
CREATE_DTM,
CREATE_DT,
QSTR,
ROW_SEQUENCE,
QST_ITEM,
QST_BODY,
QST_ANSWER,
SCORE_NBR,
TOTAL_SCORE_PRIM_QSTR,
REC_SRC,
INS_BATCH_ID

FROM (
    SELECT 
    {{ dbt_utils.surrogate_key(['QUESTIONNAIRE_ANSWER_WID'])}} as JOB_APP_QSTR_PK,
    COALESCE(CANDIDATE.CAND_APP_SK, '-1') as CAND_APP_SK,
    JOB_APPLICATION_ID as JOB_APP_ID,
    MASKED_JOB_APPLICATION_ID as MASKED_JOB_APP_ID,
    CANDIDATE_ID as CAND_ID,
    JOB_APP_WID as JOB_APP_WID,
    QUESTIONNAIRE_ANSWER_WID as QSTR_ANSWER_WID,
    JOB_APPLICATION as JOB_APP,
    JOB_REQUISITION as JOB_REQ,
    QUESTIONNAIRE_RESPONSE_CONTEXT as QR_CONTEXT,
    RESPONDENT as RESPONDENT,
    CREATE_DTM as CREATE_DTM,
    CREATE_DT as CREATE_DT,
    QUESTIONNAIRE as QSTR,
    ROW_SEQUENCE as ROW_SEQUENCE,
    QUESTION_ITEM as QST_ITEM,
    QUESTION_BODY as QST_BODY,
    QUESTIONNAIRE_ANSWER as QST_ANSWER,
    SCORE_NUM as SCORE_NBR,
    TOTAL_SCORE_PRIMARY_QUESTIONNAIRE as TOTAL_SCORE_PRIM_QSTR,
    REC_SRC as REC_SRC,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as INS_BATCH_ID,    
    ROW_NUMBER() OVER(PARTITION BY QUESTIONNAIRE_ANSWER_WID ORDER BY CREATE_DT DESC) RN
FROM JOB_APP_QUESTIONNAIRE

LEFT JOIN CANDIDATE
ON JOB_APP_QUESTIONNAIRE.JOB_APPLICATION_ID = CANDIDATE.JOBAPPID

{% if is_incremental() %}
    WHERE (JOB_APP_QUESTIONNAIRE.CREATE_DT >= '{{ get_max_event_time('CREATE_DT') }}'
        OR 
        JOB_APP_QSTR_PK IN (
        SELECT JOB_APP_QSTR_PK FROM {{ this }}
        WHERE
        CAND_APP_SK = '-1'
        )
    )
{% endif %}
)WHERE RN=1