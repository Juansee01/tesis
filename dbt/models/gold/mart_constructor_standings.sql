{{
    config(
        materialized='table',
        description='Cumulative constructor standings across the season'
    )
}}

with results as (
    select * from {{ source('silver', 'fact_results') }}
),

constructors as (
    select * from {{ source('silver', 'dim_constructors') }}
),

races as (
    select * from {{ source('silver', 'dim_races') }}
),

-- suma de puntos por constructor y carrera
race_points as (
    select
        year,
        round,
        constructor_id,
        sum(points) as race_points
    from results
    group by year, round, constructor_id
),

-- suma acumulada ordenada por ronda, la calculo aca porque el rank de abajo la necesita
-- para ordenar y T-SQL no deja meter una window function dentro del ORDER BY de otra
cumulative as (
    select
        rp.year,
        rp.round,
        rp.constructor_id,
        rp.race_points,
        sum(rp.race_points) over (
            partition by rp.year, rp.constructor_id
            order by rp.round
            rows between unbounded preceding and current row
        ) as cumulative_points
    from race_points rp
)

select
    cu.year,
    cu.round,
    rc.race_name,
    rc.country,
    cu.constructor_id,
    c.constructor_name,
    cu.race_points,
    cu.cumulative_points,
    rank() over (
        partition by cu.year, cu.round
        order by cu.cumulative_points desc
    ) as championship_position

from cumulative cu
left join constructors c
    on c.constructor_id = cu.constructor_id
left join races rc
    on rc.year = cu.year and rc.round = cu.round
