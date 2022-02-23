-- depends_on: {{ ref('raw_inventory') }}
{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}
select {{dq_unique_macro('raw_inventory','ITEMMASTER_ITEMNUMBER') }} union all
select {{dq_null_macro('raw_inventory','ITEMMASTER_ITEMNUMBER') }} union all
select {{dq_null_macro('raw_inventory','ITEMCOST_BUSINESSUNIT') }}
           
     
