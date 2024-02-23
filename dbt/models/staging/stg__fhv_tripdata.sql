with 

source as (

    select * from {{ source('staging', 'fhv_data_2019') }}

)

    select
        dispatching_base_num,
        cast(pickup_datetime as datetime) as pickup_datetime,
        cast(dropoff_datetime as datetime) as dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source
