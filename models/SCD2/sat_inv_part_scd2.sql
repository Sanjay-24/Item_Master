{%- call statement('Exist_query', fetch_result=True) -%}
    SELECT EXISTS ( SELECT * FROM information_schema.tables WHERE table_name = '!SAT_INV_PART_SCD2' )

{%- endcall -%}

{%- set Query_statement = load_result('Exist_query')['data'][0][0] -%}

{%- call statement('currenttime', fetch_result=True) -%}
    SELECT sysdate()

{%- endcall -%}

{%- set currdate = load_result('currenttime')['data'][0][0] -%}

{% if True %}

    select final.* from {{this}} as final
    left outer join
    {{ref('v_stg_inventory')}} as delta
    on final.PART_PK=delta.PART_PK
    where delta.PART_PK is null
    union all
    select delta.*,to_varchar('{{currdate}}') as Valid_from,'' as Valid_to from {{this}} as final
    Right outer join
    {{ref('v_stg_inventory')}} as delta
    on final.PART_PK=delta.PART_PK
    where final.PART_PK is null
    union all
    (
    select final.* from {{this}} as final
    inner join 
    {{ref('v_stg_inventory')}} as delta
    on final.PART_PK=delta.PART_PK
    where final.hashdiff = delta.hashdiff
    union all
    select delta.*,'' as Valid_from,to_varchar('{{currdate}}') as Valid_to from {{this}} as final
    inner join 
    {{ref('v_stg_inventory')}} as delta
    on final.PART_PK=delta.PART_PK
    where final.hashdiff <> delta.hashdiff
    union all
    select delta.*,final.Valid_from,to_varchar('{{currdate}}') as Valid_to from {{this}} as final
    inner join 
    {{ref('v_stg_inventory')}} as delta
    on final.PART_PK=delta.PART_PK
    where final.hashdiff <> delta.hashdiff
    )

{% else %}
	Select PART_PK,PART_HASHDIFF,PART_NAME,PART_MFGR,PART_BRAND,PART_TYPE,PART_SIZE,PART_CONTAINER,PART_RETAILPRICE,PART_COMMENT,
    to_varchar('{{currdate}}') as VALID_FROM, '' as Valid_to 
    from {{ref('v_stg_inventory')}}
    
{% endif %}

