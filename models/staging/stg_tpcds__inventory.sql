Select 
INV_DATE_SK as Date_SK, 
INV_ITEM_SK as Item_SK, 
INV_QUANTITY_ON_HAND as Quantity_on_hand, 
INV_WAREHOUSE_SK as Warehouse_SK 
FROM {{source('tpcds','inventory')}}