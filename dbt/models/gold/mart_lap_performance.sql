{{
    config(
        materialized='table',
        description='Lap performance aggregations per driver, race, and season'
    )
}}

with laps as (
    select * from {{ source('silver', 'fact_laps') }}
    where is_valid_lap = 1
),

results as (
    select * from {{ source('silver', 'fact_results') }}
),

drivers as (
    select * from {{ source('silver', 'dim_drivers') }}
)

select
    l.year,
    l.round,
    l.driver_id,
    concat(d.forename, ' ', d.surname)               as driver_name,
    r.constructor_id,
    count(*)                                         as total_valid_laps,
    min(l.lap_time)                                  as fastest_lap,
    avg(l.lap_time)                                  as avg_lap_time,
    -- consistency: lower is better (std dev of lap times excluding outliers)
    stdev(l.lap_time)                                as lap_time_stddev,
    avg(l.sector1_time)                              as avg_s1,
    avg(l.sector2_time)                              as avg_s2,
    avg(l.sector3_time)                              as avg_s3,
    r.position                                       as race_position,
    r.points                                         as points_scored,
    r.grid                                           as qualifying_position,
    r.grid - r.position                              as positions_gained

from laps l
left join results r
    on r.year = l.year and r.round = l.round and r.driver_id = l.driver_id
left join drivers d
    on d.driver_id = l.driver_id
group by
    l.year, l.round, l.driver_id, d.forename, d.surname,
    r.constructor_id, r.position, r.points, r.grid
