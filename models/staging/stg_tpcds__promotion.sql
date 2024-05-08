SELECT 
P_START_DATE_SK as start_date_SK, 
P_CHANNEL_CATALOG as Channel_catalog,
 P_CHANNEL_DEMO as channel_demo, 
 P_CHANNEL_EMAIL as channel_email,
  P_END_DATE_SK as end_date_sk, 
  P_CHANNEL_PRESS as channel_press,
   P_CHANNEL_TV as channel_tv, 
   P_DISCOUNT_ACTIVE as discount_active,
    P_CHANNEL_DETAILS as channel_details,
     P_PURPOSE as Purpose, 
     P_COST as Cost,
      P_PROMO_ID as Promo_id, 
      P_CHANNEL_EVENT as channel_event, 
      P_ITEM_SK as item_sk,
       P_RESPONSE_TARGET as response_target,
        P_PROMO_SK as promo_sk, 
        P_PROMO_NAME as promo_name,
         P_CHANNEL_DMAIL as channel_dmail, 
         P_CHANNEL_RADIO as channel_radio, 
         _AIRBYTE_NORMALIZED_AT

         From {{source('tpcds','promotion')}}