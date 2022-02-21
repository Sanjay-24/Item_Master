{%- set yaml_metadata -%}
source_model: 'raw_inventory'
derived_columns:
  primarykey: 'ItemMaster_ItemNumber'
  RECORD_SOURCE: '!JDE'
  EFFECTIVE_FROM: '!21-Feb-2022'
hashed_columns:
  ItemNumber_HID: 'ItemMaster_ItemNumber'
  CostCenterID_HID: 'CostcenterID'
  ITEM_COST_HID: 'ItemCost_ItemNumber'
  ITEM_BRANCH_Item_HID: 'ItemBranch_ItemNumber'
  ITEM_BRANCH_Costcenter_HID: 'ItemBranch_BusinessUnit'
  Item_CostCenter_HID:
    - 'ItemMaster_ItemNumber'
    - 'CostcenterID'
  ITEM_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ItemMaster_ItemNumber'
      - 'SecondItemNumber'
      - 'SourceLastUpdateDate'
      - 'ItemDescription1'
      - 'ItemDescription2'
      - 'ItemCommoditySubClass_Code'
      - 'ItemFamilyGroup_Code'
      - 'ItemDimensionGroup_Code'
      - 'ItemWarehouseProcessGroup1_Code'
      - 'ItemWarehouseProcessGroup2_Code'
      - 'ItemUnitOfMeasure_Code'
  Link_Item_CostCenter_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ItemCommodityClass_Code'
      - 'CostcenterID'
      - 'ItemMaster_ItemNumber'
      - 'ItemBranch_ItemCommoditySubClass_Code'
      - 'ItemInactiveStatus_Code'
      - 'TaxFlag'
      - 'ItemBranch_SourceLastUpdateDate'
      - 'SourceLastUpdatedBy'
      - 'IsActive'
      - 'UnitCost'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *,
       TO_DATE('{{ var('load_date') }}') AS LOAD_DATE
FROM staging