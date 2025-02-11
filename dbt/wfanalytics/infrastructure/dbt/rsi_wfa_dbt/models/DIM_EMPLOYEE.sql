{{
    config(
        materialized='table',
        tags=["core","dim","scheduled-daily"],
        full_refresh=True
    )
}}

WITH 
stgdata AS (
  select * from {{ ref('STG_WKD_EMPLOYEE_INTERIM') }}
)
, rawdata AS (
select
  RPT_EFFECTIVE_DT,
  MANAGER_ID,
  EIN,
  Worker,
  Position_ID,
  Job_Code,
  'CC_'|| trim(split_part(Cost_Center_ID,'-',1)) || trim(split_part(Cost_Center_ID,'-',2)) || trim(split_part(Cost_Center_ID,'-',3)) AS COST_CENTER_CD,
  Cost_Center_ID,
  Union_ID,
  Worker_WID,
  Worker_Type,
  Employee_Type,
  STATUS,
  Active_Status,
  Unions_Active,
  Total_FTE,
  FTE_Percent,
  Time_Type,
  Default_Weekly_Hours,
  cast(Is_Rehire as number) as is_rehire,
  Hire_DT,
  Original_Hire_DT,
  Seniority_DT,
  Continuous_service_DT_SENS,
  Time_Off_Service_DT,
  case when ACTIVE_STATUS = 1 then null else PAY_THROUGH_DT end as PAY_THROUGH_DT,
  Vesting_DT_SENS,
  case when ACTIVE_STATUS = 1 then null else Termination_DT end as TERM_DT,
  case when ACTIVE_STATUS = 1 then null else Termination_Reason_SENS end as TERM_REASON_SENS,
  Eligible_for_Rehire_SENS,
  Costing_Allocation_Exists_for_Worker,
  Pay_Group,
  Pay_Rate_Type,
  Pay_Types_Without_Payment_Election,
  Total_Base_Pay_Payroll_SENS,
  Total_Pay_Amt_SENS,
  Hourly_Rate_Amt_SENS,
  User_Name,
  Email_Work,
  Primary_Work_Mobile_Phone,
  Primary_Work_Phone,
  Work_Phones,
  Worker_has_Photo,
  Full_Legal_Name,
  Legal_Name_is_Preferred_Name,
  Legal_First_Name,
  Legal_Last_Name,
  Legal_Name_Middle_Name,
  Legal_Name_Secondary_Last_Name,
  Legal_Name_Social_Suffix,
  Legal_Name_Title,
  Preferred_Name,
  Preferred_Name_First_Name,
  Preferred_Name_Last_Name,
  Preferred_Name_Middle_Name,
  Preferred_Name_Secondary_Last_Name,
  Preferred_Name_Social_Suffix,
  Primary_Address_Formatted_Line_1_PII,
  Primary_Address_Formatted_Line_2_PII,
  Primary_Address_Formatted_Line_3_PII,
  Primary_Address_Formatted_Line_4_PII,
  Primary_Address_Postal_Cd_PII,
  Primary_Address_State_Province,
  Primary_Home_Address_City_PII,
  All_Home_Addresses_Full_with_Country_PII,
  Phone_Nums_SENS,
  Mobile_Phones_SENS,
  Email_Home_PII,
  SSN_PII,
  Other_IDs_PII,
  Birth_DT_PII,
  Generation_SENS,
  Gender_SENS,
  Ethnicity_Code_SENS,
  Hispanic_or_Latino_SENS,
  Hispanic_or_Latino_Visual_Survey_SENS,
  EEO_Race_SENS,
  EEO_Race_Visual_Survey_SENS,
  Veteran_Status_Identification_SENS,
  Latest_External_Disability_Response_Record_SENS,
  Latest_External_Disability_Status_SENS,
  Latest_External_Response_Date_of_Disability_Self_Identification_SENS,
  location,
  'LOC_'||split_part(split_part(location,'(',2),')',1)::varchar as location_id,
  row_number() over (partition by ein order by rpt_effective_dt) rn,
  row_number() over(partition by 
      MANAGER_ID,
      EIN,
      Worker,
      Position_ID,
      Job_Code,
      Cost_Center_CD,
      Cost_Center_ID,
      Union_ID,
      Worker_WID,
      Worker_Type,
      Employee_Type,
      Status,
      Active_Status,
      Unions_Active,
      Total_FTE,
      FTE_Percent,
      Time_Type,
      Default_Weekly_Hours,
      Is_Rehire,
      Hire_DT,
      Original_Hire_DT,
      Seniority_DT,
      Continuous_service_DT_SENS,
      Time_Off_Service_DT,
      Pay_Through_DT,
      Vesting_DT_SENS,
      Termination_DT,
      Termination_Reason_SENS,
      Eligible_for_Rehire_SENS,
      Costing_Allocation_Exists_for_Worker,
      Pay_Group,
      Pay_Rate_Type,
      Pay_Types_Without_Payment_Election,
      Total_Base_Pay_Payroll_SENS,
      Total_Pay_Amt_SENS,
      Hourly_Rate_Amt_SENS,
      User_Name,
      Email_Work,
      Primary_Work_Mobile_Phone,
      Primary_Work_Phone,
      Work_Phones,
      Worker_has_Photo,
      Full_Legal_Name,
      Legal_Name_is_Preferred_Name,
      Legal_First_Name,
      Legal_Last_Name,
      Legal_Name_Middle_Name,
      Legal_Name_Secondary_Last_Name,
      Legal_Name_Social_Suffix,
      Legal_Name_Title,
      Preferred_Name,
      Preferred_Name_First_Name,
      Preferred_Name_Last_Name,
      Preferred_Name_Middle_Name,
      Preferred_Name_Secondary_Last_Name,
      Preferred_Name_Social_Suffix,
      Primary_Address_Formatted_Line_1_PII,
      Primary_Address_Formatted_Line_2_PII,
      Primary_Address_Formatted_Line_3_PII,
      Primary_Address_Formatted_Line_4_PII,
      Primary_Address_Postal_Cd_PII,
      Primary_Address_State_Province,
      Primary_Home_Address_City_PII,
      All_Home_Addresses_Full_with_Country_PII,
      Phone_Nums_SENS,
      Mobile_Phones_SENS,
      Email_Home_PII,
      SSN_PII,
      Other_IDs_PII,
      Birth_DT_PII,
      Generation_SENS,
      Gender_SENS,
      Ethnicity_Code_SENS,
      Hispanic_or_Latino_SENS,
      Hispanic_or_Latino_Visual_Survey_SENS,
      EEO_Race_SENS,
      EEO_Race_Visual_Survey_SENS,
      Veteran_Status_Identification_SENS,
      Latest_External_Disability_Response_Record_SENS,
      Latest_External_Disability_Status_SENS,
      Latest_External_Response_Date_of_Disability_Self_Identification_SENS,
      location,
      location_id
      order by RPT_EFFECTIVE_DT ) as rno 
from stgdata
-- Removing contingent worker terminated records when he turns to full time employee
where 
  CONCAT(EIN,RPT_EFFECTIVE_DT) NOT IN  (
    SELECT CONCAT(EIN, RPT_EFFECTIVE_DT) AS EIN_EFF_DT FROM stgdata WHERE EIN IN (
      SELECT EIN
      FROM stgdata
      WHERE EIN IN (SELECT EIN FROM stgdata WHERE WORKER_TYPE ='Contingent Worker')
      GROUP BY EIN 
      HAVING COUNT(DISTINCT WORKER_TYPE) > 1
      ) AND WORKER_TYPE ='Contingent Worker' AND STATUS ='Terminated'
  )
)
, de_dup as (
  select *,
  row_number() over (partition by  MANAGER_ID,
  EIN,
  Worker,
  Position_ID,
  Job_Code,
  Cost_Center_CD,
  Cost_Center_ID,
  Union_ID,
  Worker_WID,
  Worker_Type,
  Employee_Type,
  Status,
  Active_Status,
  Unions_Active,
  Total_FTE,
  FTE_Percent,
  Time_Type,
  Default_Weekly_Hours,
  Is_Rehire,
  Hire_DT,
  Original_Hire_DT,
  Seniority_DT,
  Continuous_service_DT_SENS,
  Time_Off_Service_DT,
  Pay_Through_DT,
  Vesting_DT_SENS,
  TERM_DT,
  TERM_REASON_SENS,
  Eligible_for_Rehire_SENS,
  Costing_Allocation_Exists_for_Worker,
  Pay_Group,
  Pay_Rate_Type,
  Pay_Types_Without_Payment_Election,
  Total_Base_Pay_Payroll_SENS,
  Total_Pay_Amt_SENS,
  Hourly_Rate_Amt_SENS,
  User_Name,
  Email_Work,
  Primary_Work_Mobile_Phone,
  Primary_Work_Phone,
  Work_Phones,
  Worker_has_Photo,
  Full_Legal_Name,
  Legal_Name_is_Preferred_Name,
  Legal_First_Name,
  Legal_Last_Name,
  Legal_Name_Middle_Name,
  Legal_Name_Secondary_Last_Name,
  Legal_Name_Social_Suffix,
  Legal_Name_Title,
  Preferred_Name,
  Preferred_Name_First_Name,
  Preferred_Name_Last_Name,
  Preferred_Name_Middle_Name,
  Preferred_Name_Secondary_Last_Name,
  Preferred_Name_Social_Suffix,
  Primary_Address_Formatted_Line_1_PII,
  Primary_Address_Formatted_Line_2_PII,
  Primary_Address_Formatted_Line_3_PII,
  Primary_Address_Formatted_Line_4_PII,
  Primary_Address_Postal_Cd_PII,
  Primary_Address_State_Province,
  Primary_Home_Address_City_PII,
  All_Home_Addresses_Full_with_Country_PII,
  Phone_Nums_SENS,
  Mobile_Phones_SENS,
  Email_Home_PII,
  SSN_PII,
  Other_IDs_PII,
  Birth_DT_PII,
  Generation_SENS,
  Gender_SENS,
  Ethnicity_Code_SENS,
  Hispanic_or_Latino_SENS,
  Hispanic_or_Latino_Visual_Survey_SENS,
  EEO_Race_SENS,
  EEO_Race_Visual_Survey_SENS,
  Veteran_Status_Identification_SENS,
  Latest_External_Disability_Response_Record_SENS,
  Latest_External_Disability_Status_SENS,
  Latest_External_Response_Date_of_Disability_Self_Identification_SENS,
  location,
  location_id,
  rn-rno order by rpt_effective_dt) num
  from rawdata
  qualify num =1
)
, version as (
  select *, row_number() over(partition by EIN order by RPT_EFFECTIVE_DT asc) as version_number from de_dup
) 
, final as (
  select v1.*, v1.RPT_EFFECTIVE_DT as EFF_DT,  coalesce(v2.RPT_EFFECTIVE_DT -1, '9999-12-31')  as DISC_DT
  from version v1 
  left join version v2 on v1.version_number +1 =v2.version_number and v1.EIN = v2.EIN 
  order by v1.version_number
)
, base_data as (
  SELECT 
  F.EIN, EFF_DT, ROW_NUMBER() OVER(PARTITION by F.EIN order by EFF_DT asc) AS RN,
  CASE WHEN BP_EFFECTIVE_DT = EFF_DT THEN EFF_DT END AS HIRE_DT
  FROM FINAL F
  LEFT JOIN (SELECT ein, bp_effective_dt 
          FROM
              {{ source('STAGING','STG_WKD_ALL_WORKER_EVENTS') }}
          WHERE bp_type = 'Hire' and bp_transaction_status = 'Successfully Completed'
          qualify row_number() over(partition by ein, bp_effective_dt order by bp_effective_dt )=1 order by bp_effective_dt
        ) W
  ON F.EIN = W.EIN AND W.bp_effective_dt BETWEEN F.EFF_DT AND F.DISC_DT
)
, hire_date as (
  SELECT
      EIN, EFF_DT as HD_EFF_DT, MAX(HIRE_DT) OVER (PARTITION BY EIN, grouper) as HIRE_DT
  FROM
      ( SELECT EIN, EFF_DT, HIRE_DT, COUNT(HIRE_DT) OVER (PARTITION BY EIN ORDER BY EFF_DT) as grouper
        FROM base_data
      ) as grouped
  ORDER BY EIN, HD_EFF_DT
)
, trm_awe as (
  SELECT f.ein
  , stg.bp_event_completed_dt as term_completed_dt
  , stg.bp_effective_dt as term_dt
  , trim(replace(
            split_part(stg.bp_reason_sens,'>',3)
            , '(inactive)','')) as term_reason_sens
  , stg.term_eligible_for_rehire_ind_sens as eligible_for_rehire_sens
  , stg.term_pay_through_dt as pay_through_dt
  , stg.overall_bp_wid as term_wid
  , f.eff_dt as trm_awe_eff_dt
  FROM final f
  LEFT join {{ source('STAGING','STG_WKD_ALL_WORKER_EVENTS') }} stg on stg.ein = f.ein
    AND (stg.bp_event_completed_dt = f.eff_dt or stg.bp_effective_dt = f.eff_dt)
    AND stg.bp_type in ('Termination','End Contingent Worker Contract')
    AND stg.bp_transaction_status = 'Successfully Completed'
  WHERE stg.bp_effective_dt is not null
  QUALIFY row_number() over(partition by stg.ein, stg.bp_effective_dt order by stg.bp_effective_dt desc)=1
)
, model as (
SELECT 
    {{ dbt_utils.surrogate_key(['F.EIN','F.EFF_DT'])}} as EMP_SK
  ,	F.EIN AS EMP_ID
  , F.MANAGER_ID AS MGR_ID
  ,	WORKER
  ,	POSITION_ID
  ,	F.Job_Code AS JOB_CD
  --Apply business logic .. 
  , CASE WHEN substr(COST_CENTER_CD, 8,4) < '1000' THEN CONCAT(substr(COST_CENTER_CD, 1,7), '0000', SUBSTR(COST_CENTER_CD,12,3)) 
      WHEN COST_CENTER_CD is null then 'CC_20000000951' --Contingent workers
    ELSE COST_CENTER_CD END as COST_CENTER_CD
  ,	COST_CENTER_ID
  , UNION_ID
  ,	WORKER_WID
  ,	WORKER_TYPE
  ,	Employee_Type AS EMP_TYPE
  ,	STATUS
  ,	ACTIVE_STATUS
  , UNIONS_ACTIVE
  , CASE WHEN UPPER(OTHER_IDS_PII) LIKE '%ACQUIRED COMPANY%' 
      THEN SPLIT_PART(SPLIT_PART(OTHER_IDS_PII, 'Acquired Company/', 2),';',1)
    ELSE NULL END AS ACQUISITION_ID
  , TOTAL_FTE
  , FTE_Percent AS FTE_PCT
  , TIME_TYPE
  , Default_Weekly_Hours AS DEFAULT_WEEKLY_HRS
  ,	IS_REHIRE
  , CASE WHEN HD.HIRE_DT IS NULL THEN F.ORIGINAL_HIRE_DT ELSE HD.HIRE_DT END AS HIRE_DT
  ,	ORIGINAL_HIRE_DT
  ,	SENIORITY_DT
  ,	CONTINUOUS_SERVICE_DT_SENS
  , TIME_OFF_SERVICE_DT
  , CASE WHEN IS_REHIRE = 0 THEN TRM.pay_through_dt ELSE NULL END AS PAY_THROUGH_DT
  ,	VESTING_DT_SENS
  , CASE WHEN IS_REHIRE = 0 THEN TRM.term_completed_dt ELSE NULL END AS TERM_COMPLETED_DT
  , CASE WHEN IS_REHIRE = 0 THEN TRM.term_dt ELSE NULL END AS TERM_DT
  , CASE WHEN IS_REHIRE = 0 THEN TRM.term_reason_sens ELSE NULL END AS TERM_REASON_SENS
  ,	CASE WHEN IS_REHIRE = 0 THEN TRM.eligible_for_rehire_sens ELSE NULL END AS ELIGIBLE_FOR_REHIRE_SENS
  , CASE WHEN IS_REHIRE = 0 THEN TRM.term_wid ELSE NULL END AS TERM_WID
  , COSTING_ALLOCATION_EXISTS_FOR_WORKER
  , PAY_GROUP
  , PAY_RATE_TYPE
  , Pay_Types_Without_Payment_Election AS PAY_TYPES_WITHOUT_PYMT_ELECTION
  , TOTAL_BASE_PAY_PAYROLL_SENS
  , TOTAL_PAY_AMT_SENS
  , Hourly_Rate_Amt_SENS AS HRLY_RATE_AMT_SENS
  , USER_NAME
  ,	EMAIL_WORK
  , Primary_Work_Mobile_Phone AS PRIM_WORK_MOBILE_PHONE
  , Primary_Work_Phone AS PRIM_WORK_PHONE
  ,	WORK_PHONES
  ,	WORKER_HAS_PHOTO
  ,	FULL_LEGAL_NAME
  , Legal_Name_is_Preferred_Name AS LEGAL_NAME_IS_PREF_NAME
  ,	LEGAL_FIRST_NAME
  ,	LEGAL_LAST_NAME
  , Legal_Name_Middle_Name AS LEGAL_MIDDLE_NAME
  , Legal_Name_Secondary_Last_Name AS LEGAL_SECONDARY_LAST_NAME
  , Legal_Name_Social_Suffix AS LEGAL_SOCIAL_SUFFIX
  , Legal_Name_Title AS LEGAL_TITLE
  , Preferred_Name AS PREF_NAME
  , Preferred_Name_First_Name AS PREF_FIRST_NAME
  , Preferred_Name_Last_Name AS PREF_LAST_NAME
  , Preferred_Name_Middle_Name AS PREF_MIDDLE_NAME
  , Preferred_Name_Secondary_Last_Name AS PREF_SECONDARY_LAST_NAME
  , Preferred_Name_Social_Suffix AS PREF_SOCIAL_SUFFIX
  , Primary_Address_Formatted_Line_1_PII AS PRIM_ADDRESS_LINE_1_PII
  , Primary_Address_Formatted_Line_2_PII AS PRIM_ADDRESS_LINE_2_PII
  , Primary_Address_Formatted_Line_3_PII AS PRIM_ADDRESS_LINE_3_PII
  , Primary_Address_Formatted_Line_4_PII AS PRIM_ADDRESS_LINE_4_PII
  , Primary_Address_Postal_Cd_PII AS PRIM_POSTAL_CD_PII
  , Primary_Address_State_Province AS PRIM_STATE_PROVINCE
  , Primary_Home_Address_City_PII AS PRIM_CITY_PII
  , All_Home_Addresses_Full_with_Country_PII AS ALL_HOME_ADDRESSES_PII
  , Phone_Nums_SENS AS PHONE_NBRS_SENS
	, MOBILE_PHONES_SENS
	, EMAIL_HOME_PII
	, SSN_PII
	, OTHER_IDS_PII
	, BIRTH_DT_PII
	, GENERATION_SENS
	, GENDER_SENS
  , Ethnicity_Code_SENS AS ETHNICITY_CD_SENS
	, HISPANIC_OR_LATINO_SENS
	, HISPANIC_OR_LATINO_VISUAL_SURVEY_SENS
	, EEO_RACE_SENS
	, EEO_RACE_VISUAL_SURVEY_SENS
  , Veteran_Status_Identification_SENS AS VETERAN_STATUS_ID_SENS
  , Latest_External_Disability_Response_Record_SENS AS LATEST_EXT_DISABILITY_RESPONSE_REC_SENS
  , Latest_External_Disability_Status_SENS AS LATEST_EXT_DISABILITY_STATUS_SENS
  , Latest_External_Response_Date_of_Disability_Self_Identification_SENS AS LATEST_EXT_RESPONSE_DT_OF_DISABILITY_SELF_ID_SENS
  , location
  , location_id
	, RPT_EFFECTIVE_DT as RPT_EFF_DT
	, 'WKD' AS REC_SRC
  , F.EFF_DT as EFF_DT
  , F.DISC_DT as FINAL_DISC_DT
  , FALSE IS_CURR
	, to_number(to_varchar(current_timestamp, 'yyyymmddhh24miss')) as INS_BATCH_ID
	, TO_NUMBER(to_varchar(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISS')) AS UPD_BATCH_ID
  , row_number() over (partition by EMP_ID order by EFF_DT) as RN
FROM final F
LEFT JOIN hire_date HD ON HD.EIN = F.EIN AND HD.HD_EFF_DT = F.EFF_DT
LEFT JOIN trm_awe trm on trm.EIN = F.EIN and (trm.term_completed_dt = F.EFF_DT or trm.term_dt = F.EFF_DT)
)
SELECT
    EMP_SK
  ,	EMP_ID
  , MGR_ID
  ,	WORKER
  ,	POSITION_ID
  ,	JOB_CD
  , COST_CENTER_CD
  ,	COST_CENTER_ID
  , UNION_ID
  ,	WORKER_WID
  ,	WORKER_TYPE
  ,	EMP_TYPE
  ,	STATUS
  ,	ACTIVE_STATUS
  , UNIONS_ACTIVE
  , ACQUISITION_ID
  , TOTAL_FTE
  , FTE_PCT
  , TIME_TYPE
  , DEFAULT_WEEKLY_HRS
  ,	IS_REHIRE
  , HIRE_DT
  ,	ORIGINAL_HIRE_DT
  ,	SENIORITY_DT
  ,	CONTINUOUS_SERVICE_DT_SENS
  , TIME_OFF_SERVICE_DT
  , CASE WHEN PAY_THROUGH_DT IS NULL AND IS_REHIRE = 0 THEN MAX(PAY_THROUGH_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE PAY_THROUGH_DT END AS PAY_THROUGH_DT
  ,	VESTING_DT_SENS
  , CASE WHEN TERM_COMPLETED_DT IS NULL AND IS_REHIRE = 0 THEN MAX(TERM_COMPLETED_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE TERM_COMPLETED_DT END AS TERM_COMPLETED_DT
  , CASE WHEN TERM_DT IS NULL AND IS_REHIRE = 0 THEN MAX(TERM_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE TERM_DT END AS TERM_DT
  , CASE WHEN TERM_REASON_SENS IS NULL AND IS_REHIRE = 0 THEN MAX(TERM_REASON_SENS) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE TERM_REASON_SENS END AS TERM_REASON_SENS
  ,	CASE WHEN ELIGIBLE_FOR_REHIRE_SENS IS NULL AND IS_REHIRE = 0 THEN MAX(ELIGIBLE_FOR_REHIRE_SENS) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE ELIGIBLE_FOR_REHIRE_SENS END AS ELIGIBLE_FOR_REHIRE_SENS
  , CASE WHEN TERM_WID IS NULL AND IS_REHIRE = 0 THEN MAX(TERM_WID) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, RN)
    ELSE TERM_WID END AS TERM_WID
  , COSTING_ALLOCATION_EXISTS_FOR_WORKER
  , PAY_GROUP
  , PAY_RATE_TYPE
  , PAY_TYPES_WITHOUT_PYMT_ELECTION
  , TOTAL_BASE_PAY_PAYROLL_SENS
  , TOTAL_PAY_AMT_SENS
  , HRLY_RATE_AMT_SENS
  , USER_NAME
  ,	EMAIL_WORK
  , PRIM_WORK_MOBILE_PHONE
  , PRIM_WORK_PHONE
  ,	WORK_PHONES
  ,	WORKER_HAS_PHOTO
  ,	FULL_LEGAL_NAME
  , LEGAL_NAME_IS_PREF_NAME
  ,	LEGAL_FIRST_NAME
  ,	LEGAL_LAST_NAME
  , LEGAL_MIDDLE_NAME
  , LEGAL_SECONDARY_LAST_NAME
  , LEGAL_SOCIAL_SUFFIX
  , LEGAL_TITLE
  , PREF_NAME
  , PREF_FIRST_NAME
  , PREF_LAST_NAME
  , PREF_MIDDLE_NAME
  , PREF_SECONDARY_LAST_NAME
  , PREF_SOCIAL_SUFFIX
  , PRIM_ADDRESS_LINE_1_PII
  , PRIM_ADDRESS_LINE_2_PII
  , PRIM_ADDRESS_LINE_3_PII
  , PRIM_ADDRESS_LINE_4_PII
  , PRIM_POSTAL_CD_PII
  , PRIM_STATE_PROVINCE
  , PRIM_CITY_PII
  , ALL_HOME_ADDRESSES_PII
  , PHONE_NBRS_SENS
  , MOBILE_PHONES_SENS
  , EMAIL_HOME_PII
  , SSN_PII
  , OTHER_IDS_PII
  , BIRTH_DT_PII
  , GENERATION_SENS
  , GENDER_SENS
  , ETHNICITY_CD_SENS
  , HISPANIC_OR_LATINO_SENS
  , HISPANIC_OR_LATINO_VISUAL_SURVEY_SENS
  , EEO_RACE_SENS
  , EEO_RACE_VISUAL_SURVEY_SENS
  , VETERAN_STATUS_ID_SENS
  , LATEST_EXT_DISABILITY_RESPONSE_REC_SENS
  , LATEST_EXT_DISABILITY_STATUS_SENS
  , LATEST_EXT_RESPONSE_DT_OF_DISABILITY_SELF_ID_SENS
  , LOCATION
  , LOCATION_ID
  , RPT_EFF_DT
  , REC_SRC
  , EFF_DT
  , coalesce(
    CASE WHEN EFF_DT = (LEAD(EFF_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, EFF_DT)) THEN EFF_DT
    ELSE (LEAD(EFF_DT) OVER (PARTITION BY EMP_ID ORDER BY EMP_ID, EFF_DT)) - 1
    END, '9999-12-31') as DISC_DT
  , CASE WHEN DISC_DT = '9999-12-31' then TRUE else FALSE END as IS_CURR
  , INS_BATCH_ID
  , UPD_BATCH_ID
FROM model
order by emp_id, rn
