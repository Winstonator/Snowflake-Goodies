{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_FACT_GL_BUDGET8895164475594588974448916621422745083605849311') }}
where expirationdate = 0