{{
    config(
        materialized='table',
        full_refresh = True
    )
}}

WITH TR AS (
    select distinct 
        bp_type as Term_type, 
        TRIM(split_part(bp_reason_category_sens, '>' , 2)) as Term_Category,
        TRIM(split_part(bp_reason_sens,'>', 3)) as TERM_SUBCATEGORY,
        involuntary, 
        retirement 
    --add include in Turnover_Flag 
    from {{ source('STAGING','STG_WKD_ALL_WORKER_EVENTS') }}
    where bp_type in ( 'Termination','End Contingent Worker Contract')
)
    
SELECT  Term_type, 
Term_Category,
TERM_SUBCATEGORY,
involuntary, 
retirement ,
CASE WHEN TERM_SUBCATEGORY in ('Workplace Dynamics - Supervisor / Manager',
        'Workplace Dynamics - Job Dissatisfaction',
        'Work Eligibility / Licensure',
        'Unsatisfactory Performance',
        'Safety Violation(s)',
        'Personal / Career Advancement (inactive)',
        'Personal / Career Advancement',
        'Other (inactive)',
        'Misconduct - Violation Comp Policy',
        'Misconduct - Absolutes & Standards',
        'Job Abandonment',
        'Family / Personal - Work Relocation',
        'Family / Personal - Work Location / Commute',
        'Family / Personal - Flexibility (Work Schedule / Hours)',
        'Failure to Return from LOA',
        'Education - Return to School',
        'Education - Lack of Training / Certification',
        'Compensation / Benefits - Compensation to Non-Competitor',
        'Compensation / Benefits - Compensation to Competitor',
        'Compensation / Benefits - Benefits (Medical, Dental, PTO, 401k, etc.)',
        'Career Growth - Moving to Non-Competitor',
        'Career Growth - Moving to Competitor',
        'Attendance')
                THEN 1 ELSE 0 END AS TURNOVER_FILTER
FROM TR  where term_category is not null   
UNION ALL
SELECT '-1' , '-1', '-1', 0,0,0
UNION ALL
SELECT 'Termination','Resignation', 'Personal / Career Advancement',0,0,1
UNION ALL
Select 'Termination','Other','Improper ATS Processing',0,0,0
UNION ALL 
SELECT 'Termination', 'Other','Allied Merger-Severance',0,0,0
UNION ALL
SELECT 'Termination', 'Resignation', 'Other',0,0,1

