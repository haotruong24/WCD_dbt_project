Select 
SM_SHIP_MODE_SK as ship_mode_sk, 
SM_TYPE as Type, SM_CODE as Code, 
SM_CARRIER as Carrier, 
SM_CONTRACT as Contract, 
SM_SHIP_MODE_ID as ship_mode_id,
_AIRBYTE_NORMALIZED_AT
FROM {{source('tpcds','ship_mode')}}