-- depends_on: {{ ref('h_costcenters') }}
-- depends_on: {{ ref('h_items') }}
-- depends_on: {{ ref('l_item_costcenters') }}
-- depends_on: {{ ref('s_items') }}
-- depends_on: {{ ref('s_l_item_costcenters') }}

{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}
select {{dq_unique_macro('h_costcenters','COSTCENTERID_HID') }} union all
select {{dq_unique_macro('h_items','ItemNumber_HID') }} union all
select {{dq_unique_macro('l_item_costcenters','Item_CostCenter_HID') }} union all
select {{dq_unique_macro('s_items','ItemNumber_HID') }} union all
select {{dq_unique_macro('s_l_item_costcenters','Item_CostCenter_HID') }} union all
select {{dq_null_macro('h_costcenters','COSTCENTERID_HID') }} union all
select {{dq_null_macro('h_items','ItemNumber_HID') }} union all
select {{dq_null_macro('l_item_costcenters','Item_CostCenter_HID') }} union all
select {{dq_null_macro('s_items','ItemNumber_HID') }} union all
select {{dq_null_macro('s_l_item_costcenters','Item_CostCenter_HID') }} 

