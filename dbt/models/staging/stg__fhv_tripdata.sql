with 

source as (

    select * from {{ source('staging', 'fhv_data_2019') }}

)

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from source
