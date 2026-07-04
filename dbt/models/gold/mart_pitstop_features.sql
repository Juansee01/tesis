{{
    config(
        materialized='table',
        description='Feature table for the XGBoost pit stop window classifier. One row per driver per stop.'
    )
}}

with pitstops as (
    select * from {{ source('silver', 'fact_pitstops') }}
    where duration is not null
),

laps as (
    -- NOTE: no is_valid_lap filter here. The pit ("in") lap is flagged invalid in
    -- Silver, so filtering to valid laps drops every pit lap and yields 0 features.
    -- We need tyre_life / compound / lap_time at the pit lap regardless of validity.
    select * from {{ source('silver', 'fact_laps') }}
),

results as (
    select * from {{ source('silver', 'fact_results') }}
    where total_laps > 0
),

-- lap time degradation: avg of last 10 laps before the pit vs lap at pit-10
-- simplified to the difference between lap_time at pit and lap_time 10 laps prior
laps_at_pit as (
    select
        p.year,
        p.round,
        p.driver_id,
        p.stop,
        p.lap                                           as pit_lap,
        p.duration,
        l_pit.lap_time                                  as lap_time_at_pit,
        l_early.lap_time                                as lap_time_10_before,
        l_pit.compound                                  as compound_at_pit,
        l_pit.tyre_life                                 as tyre_age_at_pit,
        l_pit.stint
    from pitstops p
    left join laps l_pit
        on l_pit.year = p.year
        and l_pit.round = p.round
        and l_pit.driver_id = p.driver_id  -- note: Silver has unified driver_id
        and l_pit.lap_number = p.lap
    left join laps l_early
        on l_early.year = p.year
        and l_early.round = p.round
        and l_early.driver_id = p.driver_id
        and l_early.lap_number = p.lap - 10
),

-- circuit average pit duration (circuit-level feature)
circuit_pit_avg as (
    select
        year,
        round,
        avg(duration) as circuit_avg_pit_time_loss
    from pitstops
    group by year, round
),

-- constructor average pit stop duration (team-level feature)
constructor_pit_avg as (
    select
        r.constructor_id,
        avg(p.duration) as constructor_avg_pitstop_duration
    from pitstops p
    join results r
        on r.year = p.year and r.round = p.round and r.driver_id = p.driver_id
    group by r.constructor_id
),

-- weather: 1 = dry, 0 = wet (from silver_fact_laps track_status or a weather table)
race_weather as (
    select
        year,
        round,
        -- TrackStatus = '1' means green flag / dry; '2' = yellow; '4' = SC; '5' = red; '6' = VSC; '7' = wet
        max(case when track_status in ('1', '2', '4', '6') then 1 else 0 end) as weather_is_dry
    from laps
    group by year, round
)

select
    lap.year,
    lap.round,
    lap.driver_id,
    res.constructor_id,
    lap.compound_at_pit,
    case lap.compound_at_pit
        when 'SOFT'   then 0
        when 'MEDIUM' then 1
        when 'HARD'   then 2
        else 3
    end                                                         as compound_encoded,
    lap.tyre_age_at_pit,
    -- degradation slope: positive means times are getting worse (slower)
    coalesce(
        (lap.lap_time_at_pit - lap.lap_time_10_before) / 10.0,
        0.0
    )                                                           as lap_time_degradation_slope,
    cast(lap.pit_lap as float) / res.total_laps                 as race_progress_at_pit,
    res.grid                                                    as qualifying_position,
    lap.stop                                                    as n_stops_so_far,
    coalesce(cpa.circuit_avg_pit_time_loss, 22.0)               as circuit_avg_pit_time_loss,
    coalesce(w.weather_is_dry, 1)                               as weather_is_dry,
    coalesce(constr.constructor_avg_pitstop_duration, 22.0)     as constructor_avg_pitstop_duration,
    -- label: pit window class derived from lap number
    case
        when lap.pit_lap <= 20 then 'EARLY'
        when lap.pit_lap <= 40 then 'MID'
        else 'LATE'
    end                                                         as pit_window_class

from laps_at_pit lap
left join results res
    on res.year = lap.year and res.round = lap.round and res.driver_id = lap.driver_id
left join circuit_pit_avg cpa
    on cpa.year = lap.year and cpa.round = lap.round
left join race_weather w
    on w.year = lap.year and w.round = lap.round
left join constructor_pit_avg constr
    on constr.constructor_id = res.constructor_id

where res.total_laps > 0
  and lap.tyre_age_at_pit is not null
  and lap.tyre_age_at_pit > 0
