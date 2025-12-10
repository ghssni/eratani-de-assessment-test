{{ config(materialized='view') }}

with raw as (
    select * from {{ source('agriculture', 'stg_agriculture_raw') }}
),
renamed as (
    select
        trim(farm_id) as farm_id,
        trim(crop_type) as crop_type,
        cast(farm_area_acres as numeric) as farm_area_acres,
        trim(irrigation_type) as irrigation_type,
        cast(fertilizer_used_tons as numeric) as fertilizer_used_tons,
        cast(pesticide_used_kg as numeric) as pesticide_used_kg,
        cast(yield_tons as numeric) as yield_tons,
        trim(soil_type) as soil_type,
        trim(season) as season,
        cast(water_usage_cubic_meters as numeric) as water_usage_cubic_meters
    from raw
    where farm_id is not null
      and crop_type is not null
      and yield_tons is not null
)

select distinct * from renamed;
