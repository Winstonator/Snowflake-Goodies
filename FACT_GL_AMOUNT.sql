{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_FACT_GL_AMOUNT4691153832048177163719160045997332055528002872') }}
where expirationdate = 0