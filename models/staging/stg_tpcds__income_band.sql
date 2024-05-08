Select 
IB_LOWER_BOUND as Lower_bound, 
IB_INCOME_BAND_SK as Income_band_SK, 
IB_UPPER_BOUND as Upper_bound,  
_AIRBYTE_NORMALIZED_AT as _airbyte_normalized_at 
from {{source('tpcds','income_band')}} 