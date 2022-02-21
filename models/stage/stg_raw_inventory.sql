{%- set yaml_metadata -%}
source_model: 'raw_inventory'
derived_columns:
  primarykey: 'IMITM'
  RECORD_SOURCE: '!JDE'
  EFFECTIVE_FROM: '!21-Feb-2022'
hashed_columns:
  ItemNumber_HID: 'IMITM'
  CostCenterID_HID: 'MCMCU'
  ITEM_COST_HID: 'COITM'
  ITEM_BRANCH_Item_HID: 'IBITM'
  ITEM_BRANCH_Costcenter_HID: 'IBMCU'
  Item_CostCenter_HID:
    - 'IMITM'
    - 'MCMCU'
  ITEM_MASTER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'IMITM'
      - 'IMLITM'
      - 'IMUPMJ'
      - 'IMDSC1'
      - 'IMDSC2'
      - 'IMPRP2'
      - 'IMPRP4'
      - 'IMPRP6'
      - 'IMPRP7'
      - 'IMPRP8'
      - 'IMUOM1'
  ITEM_BRANCH_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'IBPRP1'
      - 'IBPRP2'
      - 'IBPRP5'
      - 'IBTX'
      - 'IBUPMJ'
      - 'IBUSER'
      - 'IBITM'
  Item_Cost_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'COUNCS'
      - 'COITM'
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