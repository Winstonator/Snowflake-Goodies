{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_GL_ACCT_HIER617530945238820806236758211112758594208610222347') }}
where expirationdate = 0