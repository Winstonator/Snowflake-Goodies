{{
    config(
        materialized='incremental',
        unique_key='JOB_REQ_EVENT_PK',
        full_refresh=false
        )
}}

{% do log("Running the model now", info=True) %} 

WITH
JOB_REQUISITION_EVENT AS (
    SELECT * from  {{ source('STAGING','STG_WKD_JOB_REQUISITION_EVENT') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY EVENT_WID ORDER BY BP_COMPLETED_DTM DESC) = 1
),

-- R-123456          "R-[0-9][0-9][0-9][0-9][0-9][0-9]"
-- R_41295BR_01      "R_[0-9][0-9][0-9][0-9][0-9]BR_[0-9][0-9]"
-- R_57334BR_99      see R_41295BR_01
-- R_41295BR_EVRGRN  "R_[0-9][0-9][0-9][0-9][0-9]BR_EVRGRN"
-- R-1234567         "R-[0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- R_123456          "R_[0-9][0-9][0-9][0-9][0-9][0-9]"
-- R_1234567         "R_[0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
{% set regex_list = [
                     "R-[0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"R_[0-9][0-9][0-9][0-9][0-9]BR_[0-9][0-9]"
                    ,"R_[0-9][0-9][0-9][0-9][0-9]BR_EVRGRN"
                    ,"R-[0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"R_[0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"R_[0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ]
%}
{% set regex = "'" ~ regex_list|join('|') ~ "'" %}
{% do log("regex: " ~ regex, info=True) %}
-- concat_ws: Concatenate with a separator.
-- regexp_substr_all('string','regex pattern',3,5): Find regex patterns in a given string and return them in an array (list). Start finding from 3rd position, and start returning from 5th position.
-- table(flatten(array)): Convert an array into table so that we can get distict values out of it eventually using array_agg
-- array_agg: Convert the table back into an array using distinct. You can do "within group order by". But to group by some other columns it needs to be an outer "group by". In this case "group by EVENT_WID, BP_COMPLETED_DTM"
REQID AS (
    SELECT EVENT_WID, BP_COMPLETED_DTM
    , array_agg(distinct id_table.value) as unique_id_list
    from JOB_REQUISITION_EVENT stg
    , table(flatten(
            regexp_substr_all(upper(concat_ws(' '
                            , coalesce(EVENT_BP_TRANSACTION,'')
                            , coalesce(BP_FOR_SUPERVISOR_ORG,'')
                            , coalesce(BP_JOB_REQUISTION,'')
                            )), {{ regex }},1,1)
            )) id_table
    group by EVENT_WID, BP_COMPLETED_DTM
),

-- P_12345678       "P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- P_123456789       "P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- P_1234567890      "P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- P-12345678       "P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- P-123456789       "P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
-- P-1234567890      "P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
{% set regex_list = [
                     "P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"P_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ,"P-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"
                    ]
%}
{% set regex = "'" ~ regex_list|join('|') ~ "'" %}
{% do log("regex: " ~ regex, info=True) %}
POSID AS (
    SELECT EVENT_WID, BP_COMPLETED_DTM
    , array_agg(distinct id_table.value) as unique_id_list
    from JOB_REQUISITION_EVENT stg
    , table(flatten(
            regexp_substr_all(upper(concat_ws(' '
                            , coalesce(EVENT_BP_TRANSACTION,'')
                            , coalesce(BP_FOR_SUPERVISOR_ORG,'')
                            , coalesce(BP_JOB_REQUISTION,'')
                            )), {{ regex }},1,1)
            )) id_table
    group by EVENT_WID, BP_COMPLETED_DTM
),

STG_WKD_JOB_REQUISITION_EVENT AS (
    SELECT stg.*
    , 'WKD' AS REC_SRC, 1 AS BATCH_KEY_ID
    , case array_size(nvl(REQID.unique_id_list,[]))
        when 1 then array_to_string(REQID.unique_id_list,'')
      else null end as JOB_REQ_ID
    , case array_size(nvl(POSID.unique_id_list,[]))
        when 1 then array_to_string(POSID.unique_id_list,'')
      else null end as POSITION_ID
    , array_size(nvl(REQID.unique_id_list,[])) as JOB_REQ_ID_ARRAY_SIZE
    , array_size(nvl(POSID.unique_id_list,[])) as POS_ID_ARRAY_SIZE
    from JOB_REQUISITION_EVENT stg
    left join REQID on REQID.EVENT_WID = stg.EVENT_WID and REQID.BP_COMPLETED_DTM = stg.BP_COMPLETED_DTM
    left join POSID on POSID.EVENT_WID = stg.EVENT_WID and POSID.BP_COMPLETED_DTM = stg.BP_COMPLETED_DTM
),

REQUISITION AS (
    SELECT JOB_REQ_ID, JOB_REQ_SK FROM {{ ref('DIM_REQUISITION') }}
)

SELECT
    {{ dbt_utils.surrogate_key(['EVENT_WID'])}} as JOB_REQ_EVENT_PK,
    CASE WHEN STG.JOB_REQ_ID_ARRAY_SIZE > 1 THEN '-2'
         WHEN STG.JOB_REQ_ID_ARRAY_SIZE = 0 THEN '-1'
         WHEN nvl(REQ.JOB_REQ_ID,'CHUCK') = 'CHUCK' THEN '-1'
    ELSE REQ.JOB_REQ_SK END as JOB_REQ_SK,
    CASE WHEN STG.JOB_REQ_ID_ARRAY_SIZE > 1 THEN 'MULTIPLE'
    ELSE STG.JOB_REQ_ID END as JOB_REQ_ID,
    CASE WHEN STG.POS_ID_ARRAY_SIZE > 1 THEN 'MULTIPLE'
    ELSE STG.POSITION_ID END as POSITION_ID,
    STG.JOB_REQ_EVENT_WID as JOB_REQ_EVENT_WID,
    STG.EVENT_WID as EVENT_WID,
    STG.BP_JOB_REQUISTION as BP_JOB_REQ,
    STG.BP_STATUS as BP_STATUS,
    STG.BP_TYPE as BP_TYPE,
    STG.BP_REASON_CATEGORY as BP_REASON_CATEGORY,
    STG.BP_REASON as BP_REASON,
    STG.BP_FOR_SUPERVISOR_ORG as BP_FOR_SUPERVISOR_ORG,
    STG.BP_INITIATED_DTM as BP_INITIATED_DTM,
    STG.BP_COMPLETED_DTM as BP_COMPLETED_DTM,
    STG.BP_STEP_AWAITING_ACTION as BP_STEP_AWAITING_ACTION,
    STG.EVENT_BP_DEFINITION as EVENT_BP_DEFINITION,
    STG.EVENT_BP_TRANSACTION as EVENT_BP_TRANSACTION,
    STG.EVENT_CREATED_DTM as EVENT_CREATED_DTM,
    STG.EVENT_COMPLETED_DTM as EVENT_COMPLETED_DTM,
    STG.EVENT_COMPLETED_BY_PERSON as EVENT_COMPLETED_BY_PERSON,
    STG.EVENT_COMPLETED_BY_USER as EVENT_COMPLETED_BY_USER,
    STG.EVENT_STATUS as EVENT_STATUS,
    STG.EVENT_TRANSACTION_STATUS as EVENT_TRANSACTION_STATUS,
    STG.EVENT_RECORD as EVENT_RECORD,
    STG.EVENT_ORDER as EVENT_ORDER,
    STG.EVENT_STEP_TYPE as EVENT_STEP_TYPE,
    STG.EVENT_STEP as EVENT_STEP,
    STG.EVENT_COMMENTS as EVENT_COMMENTS,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as INS_BATCH_ID,
    REC_SRC AS REC_SRC
FROM STG_WKD_JOB_REQUISITION_EVENT STG
LEFT JOIN REQUISITION REQ ON REQ.JOB_REQ_ID = STG.JOB_REQ_ID
    -- AND TO_DATE(STG.BP_COMPLETED_DTM) BETWEEN TO_DATE(REQ.EFF_DT) AND TO_DATE(REQ.DISC_DT) -- Type 1 now
{% if is_incremental() %} -- Type 1 now
    WHERE (STG.BP_COMPLETED_DTM >= '{{ get_max_event_time('BP_COMPLETED_DTM') }}'
        OR 
    JOB_REQ_EVENT_PK IN (
        SELECT JOB_REQ_EVENT_PK FROM {{ this }}
        WHERE JOB_REQ_SK='-1' )
    )
{% endif %}
