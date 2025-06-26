with dim_source as (
    select *
    from {{ ref("stg_stations_informations") }}
)
select
    *
from
    dim_source