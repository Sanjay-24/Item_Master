-- depends_on: {{ ref('raw_inventory') }}
{{ 
    config(
        materialized='table',
        tags=["tag_test"]
        )
}}
select {{dq_unique_macro('raw_inventory','ItemBU_ItemNumber,ItemBU_BusinessUnit') }} union all
select {{dq_null_macro('raw_inventory','ItemBU_ItemNumber,ItemBU_BusinessUnit') }} 

           
     
