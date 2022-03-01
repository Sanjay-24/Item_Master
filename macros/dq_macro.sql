{% macro dq_null_macro(table_name,column_name) %}
{%- call statement('column_names', fetch_result=True) -%}
        select REPLACE('{{column_name}}', ',', concat('||','''-''','||'));
{%- endcall -%}

{% if execute %}
  {%- set result = load_result('column_names')['data'][0][0] -%}
  
{% else %}
  {{ return(false) }}
{% endif %}
            BATCH_ID,  MODEL_NAME,'{{column_name}}' AS KEYNAME,             
			      {{result}}   AS KEYVALUE, 
              '{{column_name}}' AS FIELDANME, 
			      {{result}}  as FIELDVALUE, 
              '{{column_name}}'||'-NULL_VALUE in'|| '{{table_name}}' AS ISSUE ,
           'Error' as ErrorClassification from 
		    {{table_name}} where   {{result}} like '%NULL%' 
{% endmacro %}

{% macro dq_unique_macro(table_name,column_name) %}
{%- call statement('column_names', fetch_result=True) -%}
        select REPLACE('{{column_name}}', ',', concat('||','''-''','||'));
{%- endcall -%}

{% if execute %}
  {%- set result = load_result('column_names')['data'][0][0] -%}
  
{% else %}
  {{ return(false) }}
{% endif %}
   BATCH_ID, MODEL_NAME,'{{column_name}}' AS KEYNAME,
			 col AS KEYVALUE, 
			'{{column_name}}' AS FIELDANME, 
			col as FIELDVALUE, 
           '{{column_name}}'||'-DUPLICATE_VALUE in'|| '{{table_name}}' AS ISSUE , 'Error' as ErrorClassification from 
		   ( select BATCH_ID, MODEL_NAME,{{result}} as col ,count(*) from {{table_name}}
           group by BATCH_ID, MODEL_NAME,{{result}} having count(*) >1 )   
{% endmacro %}  



