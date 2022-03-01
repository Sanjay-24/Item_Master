{{ config(
    materialized='view',
    tags=["Source_system_orders"]
) }}

---definitions
--- Batch_Id is the one that is provided to dbt by ADF
--- Model_Name is the name of the fully qualified name : <Database_name>.<Schema_name>.<table_name>. Equivalent of dbt {{this}}
--- Job_Id is the one that we generate by MD5(BatchId+ModelName)
--- Table_Name is just the table name without any database and schema names


{%- call statement('model_name', fetch_result=True) -%}
        Select  LOWER('{{this}}') as model_name
{%- endcall -%}
{%- set  model_name = load_result('model_name')['data'][0][0] -%}

{%- call statement('table_name_query', fetch_result=True) -%}
        Select  LOWER(trim(split('{{this}}','.')[2],'"')) as DB_SH_TBL
{%- endcall -%}
{%- set  table_name = load_result('table_name_query')['data'][0][0] -%}


{%- call statement('Job_id_query', fetch_result=True) -%}
        Select  md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{table_name}}'))
{%- endcall -%}
{%- set  Job_id = load_result('Job_id_query') ['data'][0][0]  -%}

--Last Job Status check
{%- call statement('Last_Job_Status_check', fetch_result=True) -%}
    --return the lastest job id or 'Never_Run' if it is the first time
    SELECT job_status
    FROM   (SELECT job_status,
                Rank()
                    OVER (
                    ORDER BY start_timestamp DESC) AS OrderOfExecution
            FROM   (SELECT job_status,
                        start_timestamp
                    FROM   abc.PUBLIC.abc_job_details
                    WHERE  job_id = '{{Job_id}}'
                    UNION
                    SELECT 'Never_Run'  AS job_Status,
                        '1900-01-01' AS Start_TimeStamp))
    WHERE  orderofexecution = 1 
{%- endcall -%}  

{%  set Last_Job_Status =load_result('Last_Job_Status_check') ['data'][0][0] %}
{% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{Last_Job_Status}}') ;
{%- endset %}
{% do run_query(query) %}
{% if Last_Job_Status != 'SUCCESS' %}
    {{ Job_insert_update('INSERT','{{table_name}}', Job_id,var('batch_id')) }}



Select Distinct
Item.IMUOM1 as ItemUnitOfMeasure_Code ,
Item.IMLITM as SecondItemNumber,
Item.IMUPMJ as SourceLastUpdateDate,
Item.IMDSC1 as ItemDescription1,
Item.IMDSC2 as ItemDescription2,
Item.IMPRP2 as ItemCommoditySubClass_Code,
Item.IMPRP4 as ItemFamilyGroup_Code,
Item.IMPRP6 as ItemDimensionGroup_Code,
Item.IMPRP7 as ItemWarehouseProcessGroup1_Code,
Item.IMPRP8 as ItemWarehouseProcessGroup2_Code,
Item.IMITM as ItemNumber,
Item.IMUSER as SourceLastUpdatedBy,
BusinessUnit.MCMCU as BusinessUnit,
Branch.IBMCU as ItemBranch_BusinessUnit,
Branch.IBPRP1 as ItemCommodityClass_Code,
Branch.IBPRP2 as ItemBranch_ItemCommoditySubClass_Code,
Branch.IBPRP5 as ItemInactiveStatus_Code,
Branch.IBTX as TaxFlag,
Branch.IBUPMJ as ItemBranch_SourceLastUpdateDate,
Branch.IBUSER as ItemBranch_SourceLastUpdatedBy,
IFF(RTRIM(LTRIM(Branch.IBPRP5)) ='', 1, 0) AS IsActive,
Branch.IBITM as ItemBranch_ItemNumber,
ItemBU.COUNCS as UnitCost,
ItemBU.COITM as ItemBU_ItemNumber,
ItemBU.COMCU as ItemBU_BusinessUnit,
'JDE' as RECORD_SOURCE,
'{{Job_id}}' as job_id,
'{{var('batch_id')}}' as batch_id,
'{{table_name}}' as model_name
from 
{{ source('inventory', 'STG_JDE_F4101') }} as Item
left join 
{{ source('inventory', 'STG_JDE_F4102') }} as Branch
on Item.IMITM = Branch.IBITM
left join
{{ source('inventory', 'STG_JDE_F0006') }} as BusinessUnit
on Branch.IBMCU = BusinessUnit.MCMCU
left join
{{ source('inventory', 'STG_JDE_F4105') }} as ItemBU
on ItemBU.COITM = Item.IMITM and ItemBU.COMCU = BusinessUnit.MCMCU

--AND UPPER('{{Last_Job_Status}}')<>'SUCCESS'
{{ GetJobStatisticMacro(Job_id,table_name) }}


{% else %}
-- this part is there in the code else there would be no ouput for the model so the create statement will fail
-- the below code will ensure that the table wil have the data as is 
    select * from {{this}} 
{{ GetJobStatisticMacro(Job_id,table_name) }}
{% endif %}