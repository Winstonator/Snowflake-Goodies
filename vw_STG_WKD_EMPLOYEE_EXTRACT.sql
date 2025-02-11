{{
    config(
        materialized='view'
    )
}}

select * from {{ source('STAGING','STG_WKD_EMPLOYEE_EXTRACT')}}
  union 
select * from {{ source('STAGING','STG_WKD_EMPLOYEE_EXTRACT_INITIAL_BULK_LOAD')}}