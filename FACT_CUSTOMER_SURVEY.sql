{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_FACT_CUSTOMER_SURVEY3092338126443464594840047527059059897454') }}
where expirationdate = 0