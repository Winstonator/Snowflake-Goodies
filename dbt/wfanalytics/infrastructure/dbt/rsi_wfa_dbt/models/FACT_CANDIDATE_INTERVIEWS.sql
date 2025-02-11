{{
    config(
        materialized='incremental',
        unique_key='cand_interview_pk'
        
        )
}}

with

raw_data as (
    select * from {{ source('STAGING', 'STG_WKD_CANDIDATE_INTERVIEWS') }}
    {% if is_incremental() %}
        where (interview_bp_completed_dtm >= '{{ get_max_event_time('interview_bp_completed_dtm') }}'
            or
            interview_wid in (
            select feedback_wid from {{ this }}
            where cand_app_sk='-1' or job_req_sk='-1' or agency_sk='-1' or emp_sk='-1')
        )
    {% endif %}
)

, dedup_raw_data as (
    --removing duplicates
    select *, row_number() over(partition by interview_wid, interviewer_ein, job_application_wid order by interview_bp_completed_dtm desc ) as rn
    from raw_data
    qualify rn = 1  
)

, events as (
    --renaming fields based on business terms
    --integrating dimensions 
    select
        -- columns start
        {{ dbt_utils.surrogate_key(['interview_wid','interviewer_ein','job_application_wid' ])}} as cand_interview_pk,
        coalesce(can.cand_app_sk,'-1') as cand_app_sk,
        coalesce(req.job_req_sk,'-1') as job_req_sk,
        coalesce(emp.emp_sk,'-1') as emp_sk,
        coalesce(agn.agency_sk,'-1') as agency_sk,
        interview_wid as feedback_wid,
        interview_bp_wid as interview_bp_wid,
        job_application_wid as job_app_wid,
        job_application_id as job_app_id,
        job_app_masked_id as job_app_masked_id,
        job_requisition_id as job_req_id,
        candidate_id as cand_id,
        recruiting_agency_id as agency_name,
        interviewer_ein as interviewer_emp_id,
        interview_dt as feedback_interview_dt,
        interview_rating_submitted_dtm as feedback_rating_submitted_dtm,
        earliest_scheduled_interview_dtm as feedback_earliest_scheduled_interview_dtm,
        interview_bp_completed_dtm as interview_bp_completed_dtm,
        interview_bp_type as interview_bp_type,
        actual_interviewer_name_and_status as interview_feedback_name_and_status,
        interview_event_id_sens as feedback_event_id_sens,
        interview_rating_avg_sens as interview_rating_avg_sens,
        interview_rating_overall_vs_competency_sens as feedback_rating_overall_vs_competency_sens,
        interview_overall_rating_sens as feedback_overall_rating_sens,
        interview_competency_rating_sens as feedback_competency_rating_sens,
        'WKD' as rec_src,
        (select to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss'))) as ins_batch_id
    from dedup_raw_data stg
    left join {{ ref('DIM_REQUISITION') }} req on req.job_req_id = stg.job_requisition_id
    left join {{ ref('DIM_CANDIDATE') }} can on can.cand_id = stg.candidate_id  and can.job_app_id = stg.job_application_id
    left join {{ ref('DIM_AGENCY') }} agn on agn.agency_name = stg.recruiting_agency_id
    left join {{ ref('DIM_EMPLOYEE') }} emp on stg.interviewer_ein=emp.emp_id
)

select *  
from events
