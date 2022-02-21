{%- set source_model = "stg_raw_inventory" -%}
{%- set src_pk = "Item_CostCenter_HID" -%}
{%- set src_fk = ["ItemNumber_HID", "CostCenterID_HID"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                 src_source=src_source, source_model=source_model) }}
