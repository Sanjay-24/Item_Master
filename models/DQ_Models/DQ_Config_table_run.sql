{%- call statement('DQ_CONFIG', fetch_result=True) -%}
    SELECT Query FROM "DEMO_DB"."TEST"."DQ_CONFIG"
{%- endcall -%}

{%- set Query_statement = load_result('DQ_CONFIG')['data'][0][0] -%}

{%- call statement('DQ_Check', fetch_result=True) -%}
    UPDATE 'DEMO_DB'.'TEST'.'DQ_CONFIG' SET OUTPUT_RESULT = SELECT * FROM ( {{Query_statement}});

{%- endcall -%}

{%- set query_result = load_result('DQ_Check')['data'][0][0] -%}


select 1 as temp
