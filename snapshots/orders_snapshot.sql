{% snapshot orders_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='ORDER_PK',
          check_cols=['ORDERSTATUS', 'ORDER_COMMENT'],
        )
    }}

    select * from {{ ref('sat_order_order_details') }}

{% endsnapshot %}