-- depends_on: {{ ref('h_BusinessUnit') }}
-- depends_on: {{ ref('h_item') }}
-- depends_on: {{ ref('l_BusinessUnit_item') }}
-- depends_on: {{ ref('s_item') }}
-- depends_on: {{ ref('s_BusinessUnit_item') }}

{{ 
    config(
        materialized='table',
        tags=["tag_test"]
        )
}}
select {{dq_unique_macro('h_BusinessUnit','COSTCENTERID_HID') }} union all
select {{dq_unique_macro('h_item','ItemNumber_HID') }} union all
select {{dq_unique_macro('l_BusinessUnit_item','Item_CostCenter_HID') }} union all
select {{dq_unique_macro('s_item','ItemNumber_HID') }} union all
select {{dq_unique_macro('s_BusinessUnit_item','Item_CostCenter_HID') }} union all
select {{dq_null_macro('h_BusinessUnit','COSTCENTERID_HID') }} union all
select {{dq_null_macro('h_item','ItemNumber_HID') }} union all
select {{dq_null_macro('l_BusinessUnit_item','Item_CostCenter_HID') }} union all
select {{dq_null_macro('s_item','ItemNumber_HID') }} union all
select {{dq_null_macro('s_BusinessUnit_item','Item_CostCenter_HID') }} 

