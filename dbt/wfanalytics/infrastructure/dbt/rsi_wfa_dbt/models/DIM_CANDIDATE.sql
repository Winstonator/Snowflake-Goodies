{{
    config(
        materialized='incremental',
        unique_key ='CAND_APP_SK',
        full_refresh = false
        )
}}

WITH
STG_CANDIDATE AS (
    SELECT stg.*
    , 'WKD' AS REC_SOURCE
    , GREATEST( nvl(stg.JOB_APPLIED_DT, '1900-01-01')
              , nvl(stg.LAST_JOB_APPLICATION_UPDATE_DT, '1900-01-01')
              , nvl(stg.CANDIDATE_INFO_LAST_UPDATE_DT, '1900-01-01')
              ) AS PAR_COLUMN
    {% if is_incremental() %}
    , dim.cand_app_sk
    , dim.JOB_APP_DT
    , dim.LAST_JOB_APP_UPD_DT
    , dim.CAND_INFO_LAST_UPD_DT
    {% endif %}
    FROM {{ source('STAGING','STG_WKD_CANDIDATE_PROFILE') }} stg
    {% if is_incremental() %}
    left join {{this}} dim on dim.cand_app_sk = {{ dbt_utils.surrogate_key(['JOB_APPLICATION_ID', 'CANDIDATE_ID']) }}
    {% endif %}
    WHERE 1=1
    {% if is_incremental() %}
    AND 
    (  nvl(stg.JOB_APPLIED_DT, '1900-01-01') >= nvl(dim.JOB_APP_DT, '1900-01-01')
    OR nvl(stg.LAST_JOB_APPLICATION_UPDATE_DT, '1900-01-01') >= nvl(dim.LAST_JOB_APP_UPD_DT, '1900-01-01')
    OR nvl(stg.CANDIDATE_INFO_LAST_UPDATE_DT, '1900-01-01') >= nvl(dim.CAND_INFO_LAST_UPD_DT, '1900-01-01')
    )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY stg.JOB_APPLICATION_ID, stg.CANDIDATE_ID ORDER BY PAR_COLUMN DESC) = 1
)

SELECT
    {{ dbt_utils.surrogate_key(['JOB_APPLICATION_ID','CANDIDATE_ID']) }} AS CAND_APP_SK,
    CANDIDATE_ID as CAND_ID,
    JOB_APPLICATION_ID as JOB_APP_ID,
    MASKED_JOB_APPLICATION_ID as MASKED_JOB_APP_ID,
    JOB_REQUISITION_REF_ID as JOB_REQ_ID,
    JOB_APPLICATION_WID as JOB_APP_WID,
    JOB_APPLICATION as JOB_APP,
    JOB_REQUISITION as JOB_REQ,
    CANDIDATE_ACTIVE as CAND_ACTIVE,
    CANDIDATE_LATEST_JOB_APP_STAGE as CAND_LATEST_JOB_APP_STAGE,
    JOB_APPLIED_DT as JOB_APP_DT,
    LAST_JOB_APPLICATION_UPDATE_DT as LAST_JOB_APP_UPD_DT,
    CANDIDATE_INFO_LAST_UPDATE_DT as CAND_INFO_LAST_UPD_DT,
    FULL_NAME as FULL_NAME,
    FIRST_NAME as FIRST_NAME,
    LAST_NAME as LAST_NAME,
    PREFERRED_FULL_NAME as PREF_FULL_NAME,
    PREFERRED_FIRST_NAME as PREF_FIRST_NAME,
    PREFERRED_LAST_NAME as PREF_LAST_NAME,
    LEGAL_FULL_NAME as LEGAL_FULL_NAME,
    LEGAL_FIRST_NAME as LEGAL_FIRST_NAME,
    LEGAL_LAST_NAME as LEGAL_LAST_NAME,
    NATIONAL_ID_PII as NATIONAL_ID_PII,
    CANDIDATE_LOCATION_PII as CAND_LOCATION_PII,
    CANDIDATE_ADDRESS_PII as CAND_ADDRESS_PII,
    CANDIDATE_CITY_PII as CAND_CITY_PII,
    COUNTRY_POSTAL_CODE_PII as COUNTRY_POSTAL_CD_PII,
    PHONE_NUMBER_PII as PHONE_NBR_PII,
    EMAIL_PII as EMAIL_PII,
    TERMS_AND_CONDITIONS_CHECK_BOX_STATEMENT as TERMS_AND_CONDITIONS_CHECK_BOX_STATEMENT,
    CANDIDATE_CONFIDENTIAL_IND::BOOLEAN as CAND_CONFIDENTIAL_IND,
    CANDIDATE_BIRTH_DT_PII as CAND_BIRTH_DT_PII,
    CANDIDATE_GENDER_SENS as CAND_GENDER_SENS,
    CANDIDATE_RACE_ETHNICITY_SENS as CAND_RACE_ETHNICITY_SENS,
    CANDIDATE_VETERAN_STATUS_SENS as CAND_VETERAN_STATUS_SENS,
    CANDIDATE_DISABILITY_STATUS_SENS as CAND_DISABILITY_STATUS_SENS,
    JOB_APPLICATION_UPDATES as JOB_APP_UPD,
    CANDIDATE_INFO_LAST_UPDATE_BY as CAND_INFO_LAST_UPD_BY,
    UPDATE_DISABILITY_STATUS_EVENT as UPD_DISABILITY_STATUS_EVENT,
    CANDIDATE_TYPE as CAND_TYPE,
    CANDIDATE_WORKER_TYPE as CAND_WORKER_TYPE,
    CANDIDATE_WORKER_SUB_TYPE as CAND_WORKER_SUB_TYPE,
    CANDIDATE_PRIOR_WORKER as CAND_PRIOR_WORKER,
    CANDIDATE_WITHDRAWN_IND_SENS as CAND_WITHDRAWN_IND_SENS,
    CANDIDATE_DO_NOT_HIRE_IND_SENS as CAND_DO_NOT_HIRE_IND_SENS,
    VERIFIED_EXTERNAL_CANDIDATE_ACCT_IND AS VERIFIED_EXT_CAND_ACCT_IND,
    SOURCE_CATEGORY as SRC_CATEGORY,
    SOURCE as SRC,
    JOB_APPLICATION_SOURCE as JOB_APP_SRC,
    CANDIDATE_POOLS as CAND_POOLS,
    RECRUITING_AGENCY as AGENCY,
    RECRUITING_AGENCY_USER as AGENCY_USER,
    AGENCY_CANDIDATE_SUBMISSION_DT as AGENCY_CAND_SUBMISSION_DT,
    REFERRAL as REFERRAL,
    REFERRAL_RELATIONSHIP as REFERRAL_RELATIONSHIP,
    REFERRAL_DTM as REFERRAL_DTM,
    FIRST_REFERRED_DT as FIRST_REFERRED_DT,
    REFERRAL_COMMENTS as REFERRAL_COMMENTS,
    REFERRAL_SOURCE as REFERRAL_SRC,
    REFERRED_BY_PERSON as REFERRED_BY_PERSON,
    REFERRED_BY_EMP_EIN as REFERRED_BY_EMP_ID,
    REFERRAL_JOB_REQUISITIONS as REFERRAL_JOB_REQ,
    PROSPECT_CONVERTED as PROSPECT_CONVERTED,
    ALL_ACTIVITY_STREAM_COMMENTS_PII as ALL_ACTIVITY_STREAM_COMMENTS_PII,
    RESUME_TEXT_PII as RESUME_TEXT_PII,
    REC_SOURCE AS REC_SRC,
    TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) as INS_BATCH_ID,
    TO_NUMBER(to_varchar(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID
FROM STG_CANDIDATE
