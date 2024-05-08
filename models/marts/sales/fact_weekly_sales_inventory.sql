{{config(
        materialied = 'incremental',
        unique_key=['warehouse_sk','item_sk','sold_wk_sk']
)}}

with aggregating_daily_sales_to_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    MIN(SOLD_DATE_SK) AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM(DAILY_QTY) AS SUM_QTY_WK, 
    SUM(DAILY_SALES_AMT) AS SUM_AMT_WK, 
    SUM(DAILY_NET_PROFIT) AS SUM_PROFIT_WK
FROM
    {{ref('int_sales__daily_sales')}}
GROUP BY
    1,2,4,5
),

finding_first_date_of_the_week as (
SELECT 
    WAREHOUSE_SK, 
    ITEM_SK, 
    date.d_date_sk AS SOLD_WK_SK, 
    SOLD_WK_NUM, 
    SOLD_YR_NUM, 
    SUM_QTY_WK, 
    SUM_AMT_WK, 
    SUM_PROFIT_WK
FROM
    aggregating_daily_sales_to_week daily_sales
INNER JOIN {{ref('stg_tpcds__date_dim')}} as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
and date.day_of_wk_num=0
),

-- This will help us to join with inventory table as inventory table gets populated every friday
populating_friday_date_for_the_week as (
SELECT 
    *,
    date.d_date_sk as friday_sk
FROM
    finding_first_date_of_the_week daily_sales
INNER JOIN {{ref('stg_tpcds__date_dim')}} as date
on daily_sales.SOLD_WK_NUM=date.wk_num
and daily_sales.sold_yr_num=date.yr_num
--and date.day_of_wk_num=5
)

select 
       friday.warehouse_sk, 
       friday.item_sk, 
       max(SOLD_WK_SK) as sold_wk_sk,
       sold_wk_num as sold_wk_num ,
       sold_yr_num as sold_yr_num,
       sum(sum_qty_wk) as sum_qty_wk,
       sum(sum_amt_wk) as sum_amt_wk,
       sum(sum_profit_wk) as sum_profit_wk,
       sum(sum_qty_wk)/7 as avg_qty_dy,
       sum(coalesce(inv.quantity_on_hand, 0)) as inv_qty_wk, 
       sum(coalesce(inv.quantity_on_hand, 0)) / sum(sum_qty_wk) as wks_sply,
       iff(avg_qty_dy>0 and avg_qty_dy>inv_qty_wk, true , false) as low_stock_flg_wk
from populating_friday_date_for_the_week as friday
left join {{ref('stg_tpcds__inventory')}} inv 
    on inv.date_sk = friday.friday_sk and friday.item_sk = inv.item_sk and inv.warehouse_sk = friday.warehouse_sk
group by 1, 2, 4, 5
-- extra precaution because we don't want negative or zero quantities in our final model
having sum(sum_qty_wk) > 0


{% if is_incremental() %}
 AND sold_wk_sk >= (select max(sold_wk_sk) from {{ this }})
{% endif %}