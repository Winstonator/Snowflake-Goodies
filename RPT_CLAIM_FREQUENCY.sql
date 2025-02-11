{{
    config(
        materialized='view',
        post_hook= " delete from {{ source('INBOUND_DENODO','C_RPT_CLAIM_FREQUENCY41354518787883831765779268157005836280957') }} where expirationdate <> 0"
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_RPT_CLAIM_FREQUENCY41354518787883831765779268157005836280957') }}
where expirationdate = 0