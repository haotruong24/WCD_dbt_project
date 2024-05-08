{% snapshot customer_snapshot%}

{{
    config(
        target_database = 'tpcds',
        target_schema='intermediate',
        unique_key = 'customer_sk',
        strategy='check',
        check_cols='all'  
    )
}}

select * exclude _airbyte_normalied_at from {{ref('stg_tpcds__customer')}}
{% endsnapshot %}