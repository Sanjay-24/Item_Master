{%- set source_model = "stg_raw_inventory" -%}
{%- set src_pk = "Item_CostCenter_HID" -%}
{%- set src_hashdiff = "Link_Item_CostCenter_HASHDIFF" -%}
{%- set src_payload = ["ItemCommodityClass_Code","ItemBranch_ItemCommoditySubClass_Code","ItemInactiveStatus_Code","TaxFlag","ItemBranch_SourceLastUpdateDate","SourceLastUpdatedBy","IsActive","UnitCost"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ dbtvault.sat(src_pk=src_pk, src_hashdiff=src_hashdiff,
                src_payload=src_payload, src_eff=src_eff,
                src_ldts=src_ldts, src_source=src_source,
                source_model=source_model) }}
