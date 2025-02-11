{% macro update_dim_supervisor_org_disappear() %}
    {% do log("Creating update statement", info=True) %}

        {% set update_stmt %}
            merge into {{ this }} dim
            using (	select * from
                    (select prev.org_id, max(prev.rpt_effective_dt) mx_prev_dt, max(prev.sf_insert_timestamp) sfmx
                        from {{ source('STAGING','STG_WKD_SUPERVISOR_ORG') }} prev
                        left join {{ source('STAGING','STG_WKD_SUPERVISOR_ORG') }} curr on prev.org_id=curr.org_id
                        and prev.rpt_effective_dt = dateadd(d,-1,curr.rpt_effective_dt)
                        where curr.org_id is null
                        group by prev.org_id)
                    where mx_prev_dt <> (select max(rpt_effective_dt) from {{ source('STAGING','STG_WKD_SUPERVISOR_ORG') }})
                ) as logic_cte
            on dim.org_id = logic_cte.org_id
            when matched and dim.disc_dt = '9999-12-31'
            then
            update set dim.disc_dt = logic_cte.mx_prev_dt;
        {% endset %}
        
    {% do log("Sending update to model", info=True) %}
    {{ return(update_stmt) }}

{% endmacro %}