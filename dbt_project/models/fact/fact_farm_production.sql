{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_agriculture') }}
)

select distinct
    farm_id,
    crop_type,
    cast(farm_area_acres as numeric) as farm_area_acres,
    irrigation_type,
    cast(fertilizer_used_tons as numeric) as fertilizer_used_tons,
    cast(pesticide_used_kg as numeric) as pesticide_used_kg,
    cast(yield_tons as numeric) as yield_tons,
    soil_type,
    season,
    cast(water_usage_cubic_meters as numeric) as water_usage_cubic_meters
from base;
