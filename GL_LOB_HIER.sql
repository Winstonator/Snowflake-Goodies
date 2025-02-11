{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_GL_LOB_HIER9465712740231473509270154658194558665577075706448') }}
where expirationdate = 0