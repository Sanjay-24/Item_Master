{%- call statement('Exist_query', fetch_result=True) -%}
    SELECT EXISTS ( SELECT * FROM information_schema.tables WHERE table_name = '!SAT_INV_PART_SCD2' )

{%- endcall -%}

{%- set Query_statement = load_result('Exist_query')['data'][0][0] -%}

{%- set currdate = sysdate() -%}

{% if Query_statement %}

    select final.* from {{this}} as final
    left outer join
    {{ref('v_stg_inventory')}} as delta
    on final.pk=delta.pk
    where delta.pk is null
    union all
    select delta.*,{{currdate}} as Valid_from,Null as Valid_to from {{this}} as final
    Right outer join
    {{ref('v_stg_inventory')}} as delta
    on final.pk=delta.pk
    where final.pk is null
    union all
    {
    select final.* from {{this}} as final
    inner join 
    {{ref('v_stg_inventory')}} as delta
    on final.pk=delta.pk
    where final.hashdiff = delta.hashdiff
    union all
    select delta.*,Valid_from,{{'current_time'}} as Valid_to from {{this}} as final
    inner join 
    {{ref('v_stg_inventory')}} as delta
    on final.pk=delta.pk
    where final.hashdiff <> delta.hashdiff
    }

{% else %}
	Select PART_PK,PART_HASHDIFF,PART_NAME,PART_MFGR,PART_BRAND,PART_TYPE,PART_SIZE,PART_CONTAINER,PART_RETAILPRICE,PART_COMMENT,
    sysdate() as VALID_FROM, Null as Valid_to 
    from {{ref{'v_stg_inventory'}}}
    
{% end if %}

