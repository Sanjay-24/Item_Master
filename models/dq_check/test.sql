{{ 
    config(
        materialized='table',
        tags=["tt"]
        )
}}
select {{test_macro_unique('raw_inventory','ItemBU_ItemNumber,ItemBU_BusinessUnit') }}

