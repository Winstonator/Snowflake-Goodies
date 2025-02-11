{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_GL_LOB_CATEGORY764676357917747867360594763475623739855457042') }}
where expirationdate = 0