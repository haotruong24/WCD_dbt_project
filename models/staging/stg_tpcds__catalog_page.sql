Select 
CP_DESCRIPTION as Description, 
CP_CATALOG_PAGE_NUMBER as Catalog_page_number, 
CP_TYPE as Type,
 CP_DEPARTMENT as Department, 
CP_CATALOG_PAGE_SK as Catalog_page, 
CP_CATALOG_PAGE_ID as Catalog_page_id, 
CP_CATALOG_NUMBER as Catalog_number, 
CP_END_DATE_SK as End_date_SK, 
CP_START_DATE_SK as Start_date_sk,
_AIRBYTE_NORMALIZED_AT as _airbyte_normalized_at
FROM {{source('tpcds','catalog_page')}}