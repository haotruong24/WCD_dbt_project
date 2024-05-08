SELECT
WP_CHAR_COUNT as char_count, 
WP_WEB_PAGE_ID as web_page_id, 
WP_WEB_PAGE_SK as web_page_sk, 
WP_CUSTOMER_SK as customer_sk, 
WP_AUTOGEN_FLAG as autogen_flag, 
WP_URL as url, 
WP_REC_START_DATE as rec_start_date, 
WP_IMAGE_COUNT as image_count,
 WP_TYPE as type, 
 WP_CREATION_DATE_SK as creation_date_sk, 
 WP_LINK_COUNT as link_count, 
 WP_MAX_AD_COUNT as MAX_ad_count, 
 WP_ACCESS_DATE_SK as Access_date_sk, 
 _AIRBYTE_NORMALIZED_AT
 from {{source('tpcds','web_page')}}