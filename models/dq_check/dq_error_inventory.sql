{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}
-- check for unique key
{%- set column_names = ['ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT']    -%}
{% for column_name in column_names %}

       select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'{{column_name}}' AS KEYNAME,
			 ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT AS KEYVALUE, 
            '{{column_name}}' AS FIELDANME, ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT as FIELDVALUE, 
           '{{column_name}}'||'-DUPLICATE_VALUE in STG' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT,count(*) from {{ref('raw_inventory')}}
           group by ITEMMASTER_ITEMNUMBER,ITEMCOST_BUSINESSUNIT having count(*) >1 )  union all 
		   

{% endfor %} 

-- check for null 
{% for column_name in column_names %}
         select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'{{column_name}}' AS KEYNAME,
			 ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT AS KEYVALUE, 
            '{{column_name}}' AS FIELDANME, ITEMMASTER_ITEMNUMBER||'-'||ITEMCOST_BUSINESSUNIT as FIELDVALUE, 
           '{{column_name}}'||'-NULL_VALUE' AS ISSUE , 'Error' as ErrorClassification   
           FROM {{ref('raw_inventory')}}
           where ( ITEMMASTER_ITEMNUMBER  is  null or ITEMCOST_BUSINESSUNIT is null )
            union all 
{% endfor %}       

-- check for hub vault  duplicate records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'CostCenterID_HID' AS KEYNAME,
			 to_varchar(CostCenterID_HID) AS KEYVALUE, 
            'CostCenterID_HID' AS FIELDANME, to_varchar(CostCenterID_HID) as FIELDVALUE, 
           'CostCenterID_HID'||'-DUPLICATE_VALUE in'|| '{{ref('h_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select CostCenterID_HID,count(*) from {{ref('h_costcenters')}}
           group by CostCenterID_HID having count(*) >1 )  union all  
select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ItemNumber_HID' AS KEYNAME,
			 to_varchar(ItemNumber_HID) AS KEYVALUE, 
            'ItemNumber_HID' AS FIELDANME, to_varchar(ItemNumber_HID) as FIELDVALUE, 
           'ItemNumber_HID'||'-DUPLICATE_VALUE in'|| '{{ref('h_items')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select ItemNumber_HID,count(*) from {{ref('h_items')}}
           group by ItemNumber_HID having count(*) >1 )  union all 

-- check for hub vault  null records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'CostCenterID_HID' AS KEYNAME,
			 to_varchar(CostCenterID_HID) AS KEYVALUE, 
            'CostCenterID_HID' AS FIELDANME, to_varchar(CostCenterID_HID) as FIELDVALUE, 
           'CostCenterID_HID'||'-NULL_VALUE in'|| '{{ref('h_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		    {{ref('h_costcenters')}} where CostCenterID_HID is null
            union all  
select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ItemNumber_HID' AS KEYNAME,
			 to_varchar(ItemNumber_HID) AS KEYVALUE, 
            'ItemNumber_HID' AS FIELDANME, to_varchar(ItemNumber_HID) as FIELDVALUE, 
           'ItemNumber_HID'||'-NULL_VALUE in'|| '{{ref('h_items')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   {{ref('h_items')}} where ItemNumber_HID is null 
           union all   



-- check for Link  duplicate records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'Item_CostCenter_HID' AS KEYNAME,
			 to_varchar(Item_CostCenter_HID) AS KEYVALUE, 
            'Item_CostCenter_HID' AS FIELDANME, to_varchar(Item_CostCenter_HID) as FIELDVALUE, 
           'Item_CostCenter_HID'||'-DUPLICATE_VALUE in'|| '{{ref('l_item_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select Item_CostCenter_HID,count(*) from {{ref('l_item_costcenters')}}
           group by Item_CostCenter_HID having count(*) >1 )  union all  


-- check for Link  null records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'Item_CostCenter_HID' AS KEYNAME,
			 to_varchar(Item_CostCenter_HID) AS KEYVALUE, 
            'Item_CostCenter_HID' AS FIELDANME, to_varchar(Item_CostCenter_HID) as FIELDVALUE, 
           'Item_CostCenter_HID'||'-NULL_VALUE in'|| '{{ref('l_item_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		    {{ref('l_item_costcenters')}} where Item_CostCenter_HID is null
            union all  
  		   
-- check for satellite  duplicate records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ItemNumber_HID' AS KEYNAME,
			 to_varchar(ItemNumber_HID) AS KEYVALUE, 
            'ItemNumber_HID' AS FIELDANME, to_varchar(ItemNumber_HID) as FIELDVALUE, 
           'ItemNumber_HID'||'-DUPLICATE_VALUE in'|| '{{ref('s_items')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select ItemNumber_HID,count(*) from {{ref('s_items')}}
           group by ItemNumber_HID having count(*) >1 )  union all  
select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'Item_CostCenter_HID' AS KEYNAME,
			 to_varchar(Item_CostCenter_HID) AS KEYVALUE, 
            'Item_CostCenter_HID' AS FIELDANME, to_varchar(Item_CostCenter_HID) as FIELDVALUE, 
           'Item_CostCenter_HID'||'-DUPLICATE_VALUE in'|| '{{ref('s_l_item_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select Item_CostCenter_HID,count(*) from {{ref('s_l_item_costcenters')}}
           group by Item_CostCenter_HID having count(*) >1 )  union all 

-- check for satellite  null records 

 select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'ItemNumber_HID' AS KEYNAME,
			 to_varchar(ItemNumber_HID) AS KEYVALUE, 
            'ItemNumber_HID' AS FIELDANME, to_varchar(ItemNumber_HID) as FIELDVALUE, 
           'ItemNumber_HID'||'-NULL_VALUE in'|| '{{ref('s_items')}}' AS ISSUE , 'Error' as ErrorClassification from 
		    {{ref('s_items')}} where ItemNumber_HID is null
            union all  
select 'test' as BATCH_ID,'test_model' as MODEL_NAME,'Item_CostCenter_HID' AS KEYNAME,
			 to_varchar(Item_CostCenter_HID) AS KEYVALUE, 
            'Item_CostCenter_HID' AS FIELDANME, to_varchar(Item_CostCenter_HID) as FIELDVALUE, 
           'Item_CostCenter_HID'||'-NULL_VALUE in'|| '{{ref('s_l_item_costcenters')}}' AS ISSUE , 'Error' as ErrorClassification from 
		   {{ref('s_l_item_costcenters')}} where Item_CostCenter_HID is null 
         		   		   
		   