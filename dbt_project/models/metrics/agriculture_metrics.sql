{{ config(materialized='table', alias='agriculture_metrics_daily') }}

with fact as (
    select * from {{ ref('fact_farm_production') }}
),

total_yield as (
    select
        current_date as as_of_date,
        'total_yield_by_crop_and_season' as metric,
        crop_type,
        season,
        null::text as irrigation_type,
        sum(yield_tons) as value,
        'tons' as unit,
        null::int as rank
    from fact
    group by crop_type, season
),

yield_per_acre as (
    select
        current_date as as_of_date,
        'yield_per_acre_by_crop' as metric,
        crop_type,
        null::text as season,
        null::text as irrigation_type,
        sum(yield_tons) / nullif(sum(farm_area_acres), 0) as value,
        'tons_per_acre' as unit,
        null::int as rank
    from fact
    group by crop_type
),

fertilizer_efficiency as (
    select
        current_date as as_of_date,
        'fertilizer_efficiency_by_crop' as metric,
        crop_type,
        null::text as season,
        null::text as irrigation_type,
        sum(yield_tons) / nullif(sum(fertilizer_used_tons), 0) as value,
        'tons_per_ton_fertilizer' as unit,
        null::int as rank
    from fact
    group by crop_type
),

water_productivity as (
    select
        current_date as as_of_date,
        'water_productivity_by_crop' as metric,
        crop_type,
        null::text as season,
        null::text as irrigation_type,
        sum(yield_tons) / nullif(sum(water_usage_cubic_meters), 0) as value,
        'tons_per_cubic_meter' as unit,
        null::int as rank
    from fact
    group by crop_type
),

yield_per_acre_ranked as (
    select
        as_of_date,
        metric,
        crop_type,
        season,
        irrigation_type,
        value,
        unit,
        rank() over (order by value desc) as rank
    from yield_per_acre
),

top3_crops as (
    select * from yield_per_acre_ranked where rank <= 3
),

avg_yield_by_irrigation as (
    select
        current_date as as_of_date,
        'avg_yield_by_irrigation' as metric,
        null::text as crop_type,
        null::text as season,
        irrigation_type,
        avg(yield_tons) as value,
        'tons' as unit,
        null::int as rank
    from fact
    group by irrigation_type
),

top3_irrigation as (
    select
        as_of_date,
        'top3_irrigation_methods_by_avg_yield' as metric,
        crop_type,
        season,
        irrigation_type,
        value,
        unit,
        rank() over (order by value desc) as rank
    from avg_yield_by_irrigation
),

top3_irrigation_limited as (
    select * from top3_irrigation where rank <= 3
)

select * from total_yield
union all
select * from yield_per_acre
union all
select * from fertilizer_efficiency
union all
select * from water_productivity
union all
select * from top3_crops
union all
select * from top3_irrigation_limited;
