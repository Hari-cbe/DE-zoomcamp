{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select * from {{ ref('stg__fhv_tripdata') }}
    where pulocationid is not null and dolocationid is not null
),
dimZones as (
    select * from {{ ref('dim_zones') }}
)
select 

    fhv.dispatching_base_num,
    fhv.pickup_datetime,
    fhv.dropoff_datetime,
    fhv.pulocationid,
    dz.borough as pickup_location,
    dz.zone as pickup_zone,
    fhv.dolocationid,
    dz2.borough as dropoff_location,
    dz2.zone as dropoff_zone,
    fhv.sr_flag,
    fhv.affiliated_base_number

from fhv_trips as fhv
inner join dimZones as dz on fhv.pulocationid = dz.locationid
inner join dimZones as dz2 on fhv.dolocationid = dz2.locationid