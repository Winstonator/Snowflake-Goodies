{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_GL_ACCT_CATEGORY41822191382448977727429939268676679283319120') }}
where expirationdate = 0