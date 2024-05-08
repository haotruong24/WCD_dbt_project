Select WAREHOUSE_SK,
item_sk,
sold_date_sk,
daily_sales_w_customersk.Customer_sk,
SOLD_YR_NUM,
sold_wk_num,
daily_qty,
daily_sales_amt,
daily_net_profit,
Birth_year,
Country,
CREDIT_RATING
from  {{ref('int_sales__daily_sales_w_customersk')}} as daily_sales_w_customersk
left join {{ref('customer_dim')}} customer_dim on  daily_sales_w_customersk.Customer_sk = customer_dim.Customer_sk