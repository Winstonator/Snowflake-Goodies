{% macro update_dim_location_disappear() %}
    {% do log("Creating update statement", info=True) %}

        {% set update_stmt %}
            merge into {{ this }} dim
            using (	select * from
                    (select prev.location_wid, max(prev.rpt_effective_dt) mx_prev_dt, max(prev.sf_insert_timestamp) sfmx
                        from {{ source('STAGING','STG_WKD_LOCATION') }} prev
                        left join {{ source('STAGING','STG_WKD_LOCATION') }} curr on prev.location_wid=curr.location_wid
                        and prev.rpt_effective_dt = dateadd(d,-1,curr.rpt_effective_dt)
                        where curr.location_wid is null
                        group by prev.location_wid)
                    where mx_prev_dt <> (select max(rpt_effective_dt) from {{ source('STAGING','STG_WKD_LOCATION') }})
                ) as logic_cte
            on dim.location_wid = logic_cte.location_wid
            when matched and dim.disc_dt = '9999-12-31'
            then
            update set dim.disc_dt = logic_cte.mx_prev_dt;
        {% endset %}
        
    {% do log("Sending update to model", info=True) %}
    {{ return(update_stmt) }}

{% endmacro %}