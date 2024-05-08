{{
    config(
        database='tpcds'
    )
}}
select 
        SALUTATION,
        PREFERRED_CUST_FLAG,
        FIRST_SALES_DATE_SK,
        CUSTOMER_SK,
        LOGIN,
        CURRENT_CDEMO_SK,
        FIRST_NAME,
        CURRENT_HDEMO_SK,
        CURRENT_ADDR_SK,
        LAST_NAME,
        CUSTOMER_ID,
        LAST_REVIEW_DATE_SK,
        BIRTH_MONTH,
        BIRTH_COUNTRY,
        BIRTH_YEAR,
        BIRTH_DAY,
        EMAIL_ADDRESS,
        FIRST_SHIPTO_DATE_SK,
        STREET_NAME,
        SUITE_NUMBER,
        STATE,
        LOCATION_TYPE,
        COUNTRY,
        ADDRESS_ID,
        COUNTY,
        STREET_NUMBER,
        ZIP,
        CITY,
        GMT_OFFSET,
        DEP_EMPLOYED_COUNT,
        CD_DEP_COUNT,
        CREDIT_RATING,
        EDUCATION_STATUS,
        PURCHASE_ESTIMATE,
        MARITAL_STATUS,
        DEP_COLLEGE_COUNT,
        GENDER,
        BUY_POTENTIAL,
        DEP_COUNT,
        VEHICLE_COUNT,
        household_demographics.INCOME_BAND_SK,
        LOWER_BOUND,
        UPPER_BOUND
from {{ref('customer_snapshot')}}
LEFT JOIN {{ref('stg_tpcds__customer_address')}} ON current_addr_sk = address_sk
LEFT join {{ref('stg_tpcds__customer_demographics')}} as customer_demographics ON current_cdemo_sk = customer_demographics.demo_sk
LEFT join {{ref('stg_tpcds__household_demographics')}} as household_demographics ON current_hdemo_sk = household_demographics.demo_sk
LEFT join {{ref('stg_tpcds__income_band')}} as income_band ON household_demographics.income_band_sk = income_band.INCOME_BAND_SK
where customer_snapshot.dbt_valid_to is null