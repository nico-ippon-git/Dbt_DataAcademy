with informations_source as (
    select
        *
    from
        {{ ref("base_stations_informations") }}
)

, flatten_stations as (
    select
        d.value:capacity::numeric as capacity,
        d.value:lat::float as latitude,
        d.value:lon::float as longitude,
        d.value:name::varchar as name,
        d.value:stationCode::varchar as code,
        d.value:station_id::numeric as id,
        inf.data:lastUpdatedOther::timestamp as last_update
    from
        informations_source inf
    , table(flatten(input => inf.data:data.stations)) as d
)
select
    *
from
    flatten_stations