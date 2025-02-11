{% macro update_dim_security_user_based_disappear() %}
    {% do log("Creating update statement", info=True) %}

        {% set update_stmt %}

            merge into {{ this }} dim
            using ( select * from
                    (select p1.SECURITY_GROUP_REFID, P1.EMPLOYEE_ID, max(p1.rpt_effective_dt) mx_prev_dt, max(p1.sf_insert_timestamp) sfmx
                        from {{ source('STAGING','STG_WKD_SECURITY_USER_BASED') }}   p1
                        left join {{ source('STAGING','STG_WKD_SECURITY_USER_BASED') }}   p2 on p1.SECURITY_GROUP_REFID=p2.SECURITY_GROUP_REFID
                        AND P1.EMPLOYEE_ID = P2.EMPLOYEE_ID
                        and p1.RPT_EFFECTIVE_DT=dateadd(d,-1,p2.RPT_EFFECTIVE_DT)
                        where p2.SECURITY_GROUP_REFID is null AND P2.EMPLOYEE_ID IS NULL
                        group by p1.SECURITY_GROUP_REFID, P1.EMPLOYEE_ID)
                    where mx_prev_dt <> (select max(rpt_effective_dt) from {{ source('STAGING','STG_WKD_SECURITY_USER_BASED') }} )
                ) as logic_cte
            on dim.security_group_id = logic_cte.security_group_refid
            and dim.emp_id = logic_cte.employee_id
            when matched and dim.disc_dt = '9999-12-31'
            then
            update set dim.disc_dt = logic_cte.mx_prev_dt;
            
        {% endset %}
        
    {% do log("Sending update to model", info=True) %}
    {{ return(update_stmt) }}

{% endmacro %}