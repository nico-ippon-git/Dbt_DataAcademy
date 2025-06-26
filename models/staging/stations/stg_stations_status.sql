with status_source as (
    select *
    from {{ ref("base_stations_status") }}
)

-- 1. Flatten des stations
, flattened_stations as (
    select
        d.value:is_installed::numeric as is_installed,
        d.value:is_renting::numeric as is_renting,
        d.value:is_returning::numeric as is_returning,
        d.value:last_reported::numeric as last_reported,
        d.value:numBikesAvailable::numeric as num_bikes_available_total,
        d.value:numDocksAvailable::numeric as num_docks_available,
        d.value:stationCode::varchar as code,
        d.value:station_id::numeric as station_id,
        inf.data:lastUpdatedOther::timestamp as last_update,
        d.value:num_bikes_available_types as bike_types
    from status_source inf,
         table(flatten(input => inf.data:data.stations)) as d
)

-- 2. Flatten du champ bike_types
, bike_type_values as (
    select
        station_id,
        code,
        is_installed,
        is_renting,
        is_returning,
        last_reported,
        num_bikes_available_total,
        num_docks_available,
        last_update,
        b.value:mechanical::numeric as mechanical,
        b.value:ebike::numeric as ebike
    from flattened_stations,
         table(flatten(input => bike_types)) as b
)

-- 3. Agrégation pour fusionner les lignes mechanical + ebike
select
    station_id,
    code,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    num_bikes_available_total,
    num_docks_available,
    last_update,
    max(mechanical) as mechanical_bikes,
    max(ebike) as ebike_bikes
from bike_type_values
group by
    station_id,
    code,
    is_installed,
    is_renting,
    is_returning,
    last_reported,
    num_bikes_available_total,
    num_docks_available,
    last_update