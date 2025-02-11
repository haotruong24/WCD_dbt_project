Select 
WS_ORDER_NUMBER as order_number, 
WS_SHIP_MODE_SK as ship_mode_sk, 
WS_SOLD_TIME_SK as sold_time_sk, 
WS_SHIP_ADDR_SK as ship_addr_sk, 
WS_NET_PAID_INC_TAX as net_paid_inc_tax, 
WS_PROMO_SK as promo_sk, 
WS_NET_PAID_INC_SHIP as net_paid_inc_ship, 
WS_QUANTITY as quantity, 
WS_EXT_SALES_PRICE as ext_sales_price, 
WS_WEB_SITE_SK as web_site_sk, 
WS_SHIP_DATE_SK as ship_date_sk, 
WS_BILL_CDEMO_SK as bill_cdemo_sk, 
WS_NET_PROFIT as net_profit, 
WS_NET_PAID as net_paid, 
WS_EXT_SHIP_COST as ext_ship_cost, 
WS_BILL_CUSTOMER_SK as bill_customer_sk, 
WS_COUPON_AMT as Coupon_amt, 
WS_WHOLESALE_COST as Wholesale_cost,
 WS_SHIP_CUSTOMER_SK as ship_customer_sk, 
 WS_BILL_HDEMO_SK as bill_hdemo_sk, 
 WS_SOLD_DATE_SK as sold_date_sk, 
 WS_SHIP_CDEMO_SK as ship_cdemo_sk, 
 WS_WAREHOUSE_SK as warehouse_sk, 
 WS_EXT_TAX as ext_tax, 
 WS_ITEM_SK as item_sk, 
 WS_SHIP_HDEMO_SK as ship_hdemo_sk, 
 WS_EXT_WHOLESALE_COST as ext_wholesale_cost, 
 WS_NET_PAID_INC_SHIP_TAX as net_paid_inc_ship_tax,
  WS_EXT_DISCOUNT_AMT as ext_discount_amt, 
  WS_SALES_PRICE as sales_price, 
  WS_WEB_PAGE_SK as web_page_sk, 
  WS_EXT_LIST_PRICE as ext_List_price, 
  WS_LIST_PRICE as list_price, 
  WS_BILL_ADDR_SK as bill_addr_sk, 
  _AIRBYTE_NORMALIZED_AT
  From {{source('tpcds','web_sales')}}