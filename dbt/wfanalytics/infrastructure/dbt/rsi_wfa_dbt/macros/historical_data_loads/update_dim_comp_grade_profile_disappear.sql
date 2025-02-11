{% macro update_dim_comp_grade_profile_disappear() %}
    {% do log("Creating update statement", info=True) %}

        {% set update_stmt %}
            merge into {{ this }} dim
            using 
                (select * from
                    (select prev.comp_grade_profile_wid, max(prev.rpt_effective_dt) mx_prev_dt
                    from {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }} prev
                    left join {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }} curr
                        on prev.comp_grade_profile_wid = curr.comp_grade_profile_wid
                        and prev.rpt_effective_dt = dateadd(d,-1,curr.rpt_effective_dt)
                    where curr.comp_grade_profile_wid is null
                    group by prev.comp_grade_profile_wid
                    )
                where mx_prev_dt <> (select max(rpt_effective_dt) from {{ source('STAGING','STG_WKD_COMPENSATION_GRADES_PROFILES') }})
                ) as logic_cte
            on dim.comp_grade_profile_wid = logic_cte.comp_grade_profile_wid
            when matched and dim.disc_dt = '9999-12-31'
            then
            update set dim.disc_dt = logic_cte.mx_prev_dt;
        {% endset %}
        
    {% do log("Sending update to model", info=True) %}
    {{ return(update_stmt) }}

{% endmacro %}