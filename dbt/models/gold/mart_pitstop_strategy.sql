{{
    config(
        materialized='table',
        description='Pit stop strategy analysis: undercut/overcut windows per race and team'
    )
}}

with pitstops as (
    select * from {{ source('silver', 'fact_pitstops') }}
),

laps as (
    select * from {{ source('silver', 'fact_laps') }}
    where is_valid_lap = true
),

results as (
    select * from {{ source('silver', 'fact_results') }}
),

circuits as (
    select * from {{ source('silver', 'dim_circuits') }}
),

schedule as (
    select year, round, circuit_id from {{ source('silver', 'dim_circuits') }}
),

-- average pit stop duration per circuit (used as circuit_avg_pit_time_loss)
circuit_pit_avg as (
    select
        p.year,
        p.round,
        avg(p.duration) as avg_pit_duration
    from pitstops p
    where p.duration is not null
    group by p.year, p.round
),

pit_details as (
    select
        p.year,
        p.round,
        p.driver_id,
        p.stop,
        p.lap                                       as pit_lap,
        p.duration,
        r.constructor_id,
        r.position                                  as final_position,
        r.total_laps,
        cast(p.lap as float) / r.total_laps         as race_progress_at_pit,
        case
            when p.lap <= 20 then 'EARLY'
            when p.lap <= 40 then 'MID'
            else 'LATE'
        end                                         as pit_window_class
    from pitstops p
    left join results r using (year, round, driver_id)
)

select
    pd.*,
    cpa.avg_pit_duration                            as circuit_avg_pit_duration
from pit_details pd
left join circuit_pit_avg cpa using (year, round)
