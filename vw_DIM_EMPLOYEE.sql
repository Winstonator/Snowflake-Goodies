{{
    config(
        materialized='view'
    )
}}

with emp as(
select distinct d1.ein,d1.current_value ORIGINAL_HIRE_DT,d2.current_value JOB_CODE,d3.current_value HIRE_DT,d4.current_value COST_CENTER_ID,
'CC_'|| trim(split_part(cost_center_id,'-',1)) || trim(split_part(cost_center_id,'-',2)) || trim(split_part(cost_center_id,'-',3)) as COST_CENTER_CD,
d5.current_value POSITION_ID,d6.current_value TERM_DT,d7.current_value WORKER,d8.current_value USER_NAME,
greatest(d1.eff_dt,d2.eff_dt,d3.eff_dt,d4.eff_dt,d5.eff_dt,d6.eff_dt,d7.eff_dt,d8.eff_dt) eff_dt,
least(d1.disc_dt,d2.disc_dt,d3.disc_dt,d4.disc_dt,d5.disc_dt,d6.disc_dt,d7.disc_dt,d8.disc_dt) disc_dt
from {{ ref('DIM_EMPLOYEE_EVENTS') }}  d1
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d2 on d1.ein=d2.ein and coalesce(d1.CHANGE_TYPE,'')<>'Rescind' and coalesce(d2.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d3 on d1.ein=d3.ein and coalesce(d3.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d4 on d1.ein=d4.ein and coalesce(d4.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d5 on d1.ein=d5.ein and coalesce(d5.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d6 on d1.ein=d6.ein and coalesce(d6.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d7 on d1.ein=d7.ein and coalesce(d7.CHANGE_TYPE,'')<>'Rescind'
join {{ ref('DIM_EMPLOYEE_EVENTS') }}  d8 on d1.ein=d8.ein and coalesce(d8.CHANGE_TYPE,'')<>'Rescind'
where d1.change_column='ORIGINAL_HIRE_DT' and d2.change_column='JOB_CODE' and d3.change_column='HIRE_DT' and d4.change_column='COST_CENTER_ID' and d5.change_column='POSITION_ID' and d6.change_column='TERMINATION_DT' and d7.change_column='WORKER' and d8.change_column='USER_NAME'
and greatest(d1.eff_dt,d2.eff_dt,d3.eff_dt,d4.eff_dt,d5.eff_dt,d6.eff_dt,d7.eff_dt,d8.eff_dt) <=
least(d1.disc_dt,d2.disc_dt,d3.disc_dt,d4.disc_dt,d5.disc_dt,d6.disc_dt,d7.disc_dt,d8.disc_dt)
    )
select md5(ein||cast(eff_dt as varchar(10))) as emp_sk, * 
from emp    
