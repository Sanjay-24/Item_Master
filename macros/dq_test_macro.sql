{% macro dq_unique_macro1(table_name,column_name) %}
   BATCH_ID, MODEL_NAME,'{{column_name}}' AS KEYNAME,
			 to_varchar({{column_name}}) AS KEYVALUE, 
            '{{column_name}}' AS FIELDANME, to_varchar({{column_name}}) as FIELDVALUE, 
           '{{column_name}}'||'-DUPLICATE_VALUE in'|| '{{table_name}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select BATCH_ID, MODEL_NAME,{{column_name}},count(*) from {{table_name}}
           group by BATCH_ID, MODEL_NAME,{{column_name}} having count(*) >1 )   
{% endmacro %}  
{% macro dq_null_macro1(table_name,column_name) %}  
             BATCH_ID,  MODEL_NAME,'{{column_name}}' AS KEYNAME,
			 to_varchar({{column_name}}) AS KEYVALUE, 
            '{{column_name}}' AS FIELDANME, to_varchar({{column_name}}) as FIELDVALUE, 
           '{{column_name}}'||'-NULL_VALUE in'|| '{{table_name}}' AS ISSUE , 'Error' as ErrorClassification from 
		    {{table_name}} where {{column_name}} is null

{% endmacro %} 

