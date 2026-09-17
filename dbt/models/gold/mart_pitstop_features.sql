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
    -- ojo, aca no filtro por is_valid_lap. La vuelta de entrada a boxes queda marcada
    -- invalida en Silver, entonces si filtro por validas me quedo sin ninguna vuelta de
    -- pit y sin features. Necesito tyre_life/compound/lap_time de esa vuelta igual, valida o no.
    select * from {{ source('silver', 'fact_laps') }}
),

results as (
    select * from {{ source('silver', 'fact_results') }}
    where total_laps > 0
),

races as (
    select * from {{ source('silver', 'dim_races') }}
),

-- degradacion del tiempo de vuelta: comparo el lap_time en la vuelta del pit contra
-- el de 10 vueltas antes (version simplificada, no promedio las 10)
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

-- duracion promedio de pit por circuito
circuit_pit_avg as (
    select
        year,
        round,
        avg(duration) as circuit_avg_pit_time_loss
    from pitstops
    group by year, round
),

-- duracion promedio de pit por constructor
constructor_pit_avg as (
    select
        r.constructor_id,
        avg(p.duration) as constructor_avg_pitstop_duration
    from pitstops p
    join results r
        on r.year = p.year and r.round = p.round and r.driver_id = p.driver_id
    group by r.constructor_id
),

-- clima: 1 = seco, 0 = mojado (lo saco de track_status en silver_fact_laps)
race_weather as (
    select
        year,
        round,
        -- TrackStatus: '1' bandera verde/seco, '2' amarilla, '4' SC, '5' roja, '6' VSC, '7' mojado
        max(case when track_status in ('1', '2', '4', '6') then 1 else 0 end) as weather_is_dry
    from laps
    group by year, round
)

select
    lap.year,
    lap.round,
    rc.race_name,
    rc.country,
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
    -- pendiente de degradacion: si es positiva, los tiempos empeoran (mas lentos)
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
    -- label: la clase de ventana de pit segun el numero de vuelta
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
left join races rc
    on rc.year = lap.year and rc.round = lap.round

where res.total_laps > 0
  and lap.tyre_age_at_pit is not null
  and lap.tyre_age_at_pit > 0
