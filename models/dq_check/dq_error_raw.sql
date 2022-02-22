{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}
-- check for unique key

       select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT' AS KEYNAME,
			 ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT AS KEYVALUE, 
            'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT' AS FIELDANME, ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT as FIELDVALUE, 
           'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT-DUPLICATE_VALUE in STG' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT,count(*) from {{ref('raw_inventory')}}
           group by ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT having count(*) >1 )  union all 
		   
     {% set query -%}
            Insert overwrite into  "ITEM_MASTER"."DEV"."RAW_INVENTORY_DISCARDED" select * from {{ref('raw_inventory')}}
            where ( ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT) in  ( select ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT
            from {{ref('raw_inventory')}} group by ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT having count(*) >1 )
        {%- endset %}

        {% do run_query(query) %}


-- check for null 

         select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT' AS KEYNAME,
			 ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT AS KEYVALUE, 
            'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT' AS FIELDANME, ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT as FIELDVALUE, 
           'ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT-NULL_VALUE' AS ISSUE , 'Error' as ErrorClassification   
           FROM {{ref('raw_inventory')}}
           where ( ITEMMASTER_ITEMNUMBER  is  null or ITEMCOST_BUSINESSUNIT is null )
           
     
