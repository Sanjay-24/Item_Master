{%- set source_model = "stg_raw_inventory" -%}
{%- set src_pk = "CostCenterID_HID" -%}
{%- set src_nk = "CostcenterID" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}