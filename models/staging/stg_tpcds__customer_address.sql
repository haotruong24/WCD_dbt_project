Select 
CA_STREET_NAME as Street_name, 
CA_SUITE_NUMBER as Suite_number, 
CA_STATE as State, 
CA_LOCATION_TYPE as Location_type, 
CA_ADDRESS_SK as Address_SK, 
CA_COUNTRY as Country, 
CA_ADDRESS_ID as Address_id, 
CA_COUNTY as County, 
CA_STREET_NUMBER as Street_Number, 
CA_ZIP as ZIP, 
CA_CITY as City, 
CA_STREET_TYPE as Street_type, 
CA_GMT_OFFSET as GMT_offset, 
_AIRBYTE_NORMALIZED_AT as _airbyte_normalied_at
FROM {{source('tpcds','customer_address')}}