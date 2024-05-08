{{config(
        materialied = 'incremental',
        unique_key=['warehouse_sk','item_sk','sold_date_sk']
)}}

with incremental_sales as (
SELECT 
            WAREHOUSE_SK as warehouse_sk,
            ITEM_SK as item_sk,
            SOLD_DATE_SK as sold_date_sk,
            QUANTITY as quantity,
            sales_price * quantity as sales_amt,
            NET_PROFIT as net_profit,
            SHIP_CUSTOMER_SK as Customer_sk,
            Ship_mode_sk as ship_mode_sk,
            'catalog' as Sales_channel 
    from {{ref('stg_tpcds__catalog_sales')}}
    WHERE 
         quantity is not null
        and sales_amt is not null
    
    union all

    SELECT 
            WAREHOUSE_SK as warehouse_sk,
            ITEM_SK as item_sk,
            SOLD_DATE_SK as sold_date_sk,
            QUANTITY as quantity,
            sales_price * quantity as sales_amt,
            NET_PROFIT as net_profit,
            SHIP_CUSTOMER_SK as Customer_sk,
            Ship_mode_sk as ship_mode_sk,
            'web' as Sales_channel 
    from {{ref('stg_tpcds__web_sales')}}
    WHERE 
        quantity is not null
        and sales_amt is not null
)

Select warehouse_sk,
item_sk,
sold_date_sk,
quantity,sales_amt,
net_profit,
Customer_sk,
incremental_sales.ship_mode_sk,
sales_channel,
ship_mode.type,
ship_mode.carrier,
ship_mode.code

from incremental_sales
left join {{ref('stg_tpcds__ship_mode')}} ship_mode on incremental_sales.ship_mode_sk = ship_mode.ship_mode_sk

{% if is_incremental() %}
WHERE sold_date_sk > (select max(sold_date_sk) from {{ this }})
{% endif %}