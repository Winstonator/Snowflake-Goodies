{{
    config(
        materialized='view'
    )
}}

select * exclude (expirationdate, rowstatus) 
from {{ source('INBOUND_DENODO','C_DIM_COA03489555691799999991932234255599531201275509125524477') }}
where expirationdate = 0