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
            NET_PROFIT as net_profit
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
            NET_PROFIT as net_profit
    from {{ref('stg_tpcds__web_sales')}}
    WHERE 
        quantity is not null
        and sales_amt is not null
),

aggregating_records_to_daily_sales as
(
select 
    warehouse_sk,
    item_sk,
    sold_date_sk, 
    sum(quantity) as daily_qty,
    sum(sales_amt) as daily_sales_amt,
    sum(net_profit) as daily_net_profit 
from incremental_sales
group by 1, 2, 3

),

adding_week_number_and_yr_number as
(
select 
    *,
    date.wk_num as sold_wk_num,
    date.yr_num as sold_yr_num
from aggregating_records_to_daily_sales 
LEFT JOIN {{ref('stg_tpcds__date_dim')}} date 
    ON sold_date_sk = d_date_sk

)

SELECT 
	warehouse_sk,
    item_sk,
    sold_date_sk,
    max(sold_wk_num) as sold_wk_num,
    max(sold_yr_num) as sold_yr_num,
    sum(daily_qty) as daily_qty,
    sum(daily_sales_amt) as daily_sales_amt,
    sum(daily_net_profit) as daily_net_profit 
FROM adding_week_number_and_yr_number

{% if is_incremental() %}
 WHERE sold_date_sk > (select max(sold_date_sk) from {{ this }})
{% endif %}

GROUP BY 1,2,3
ORDER BY 1,2,3
