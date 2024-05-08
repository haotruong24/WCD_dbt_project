Select
 CD_DEP_EMPLOYED_COUNT as DEP_employed_count, 
 CD_DEMO_SK as Demo_SK, 
 CD_DEP_COUNT as CD_Dep_count, 
 CD_CREDIT_RATING as Credit_rating, 
 CD_EDUCATION_STATUS as Education_status, 
 CD_PURCHASE_ESTIMATE as Purchase_estimate, 
 CD_MARITAL_STATUS as Marital_status, 
 CD_DEP_COLLEGE_COUNT as Dep_college_count, 
 CD_GENDER as Gender,
 _AIRBYTE_NORMALIZED_AT as _airbyte_normalied_at
 From {{source('tpcds','customer_demographics')}}