-- depends_on: {{ ref('raw_inventory') }}
{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}
select {{dq_composite_unique_macro('raw_inventory','ITEMMASTER_ITEMNUMBER','ITEMCOST_BUSINESSUNIT') }} union all
select {{dq_composite_null_macro('raw_inventory','ITEMMASTER_ITEMNUMBER','ITEMCOST_BUSINESSUNIT') }} 

           
     
