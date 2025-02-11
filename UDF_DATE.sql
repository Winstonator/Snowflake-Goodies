{{
    config(
        materialized='table',
        pre_hook=["{{ udf_date_to_int() }}"
                ,"{{ udf_int_to_date() }}"
                ,"{{ udf_timestamp_to_int() }}"
                ,"{{ udf_int_to_timestamp() }}"],
        post_hook=["drop table if exists {{this}}"]
    )
}}  

select '1' as one


