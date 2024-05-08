Select
 HD_BUY_POTENTIAL as Buy_potential,
 HD_INCOME_BAND_SK as Income_band_SK, 
 HD_DEMO_SK as Demo_SK, 
 HD_DEP_COUNT as Dep_count, 
 HD_VEHICLE_COUNT as Vehicle_count, 
 _AIRBYTE_NORMALIZED_AT as _Airbyte_normalized_at
 From {{source('tpcds','household_demographics')}}