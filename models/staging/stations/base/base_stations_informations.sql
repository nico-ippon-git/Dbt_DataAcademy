select 
    json_data as data 
from
    {{ source("raw_data", "RAW_STATION_INFORMATION") }}