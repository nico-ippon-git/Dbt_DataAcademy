{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    cluster_by=['update_hour','station_id'],
    merge_exclude_columns=['station_id','station_code','update_hour'],
    unique_key=['update_hour','station_id']
  )
}}

with source as (
    select
        *,
        date_trunc('hour', last_update) as update_hour
    from 
        {{ ref("stg_stations_status") }}
)

select *
from source