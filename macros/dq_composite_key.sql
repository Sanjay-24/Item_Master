{% macro dq_composite_unique_macro(table_name,column_name1,column_name2) %}
   BATCH_ID, MODEL_NAME,'{{column_name1}},{{column_name2}}' AS KEYNAME,
			 nvl(to_char({{column_name1}}),'null')||','||nvl(to_char({{column_name2}}),'null') AS KEYVALUE, 
			'{{column_name1}},{{column_name2}}' AS FIELDANME, 
			nvl(to_char({{column_name1}}),'null')||','||nvl(to_char({{column_name2}}),'null') as FIELDVALUE, 
           '{{column_name1}},{{column_name2}}'||'-DUPLICATE_VALUE in'|| '{{table_name}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select BATCH_ID, MODEL_NAME,{{column_name1}},{{column_name2}},count(*) from {{table_name}}
           group by BATCH_ID, MODEL_NAME,{{column_name1}},{{column_name2}} having count(*) >1 )   
{% endmacro %}  
{% macro dq_composite_null_macro(table_name,column_name1,column_name2) %}  
             BATCH_ID,  MODEL_NAME,'{{column_name1}},{{column_name2}}' AS KEYNAME,
			 nvl(to_char({{column_name1}}),'null')||','||nvl(to_char({{column_name2}}),'null') AS KEYVALUE, 
            '{{column_name1}},{{column_name2}}' AS FIELDANME, 
			nvl(to_char({{column_name1}}),'null')||','||nvl(to_char({{column_name2}}),'null') as FIELDVALUE, 
           '{{column_name1}},{{column_name2}}'||'-NULL_VALUE in'|| '{{table_name}}' AS ISSUE , 'Error' as ErrorClassification from 
		    {{table_name}} where {{column_name1}} is null or {{column_name2}} is null

{% endmacro %} 