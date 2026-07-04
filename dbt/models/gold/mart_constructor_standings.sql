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

-- sum points per constructor per race
race_points as (
    select
        year,
        round,
        constructor_id,
        sum(points) as race_points
    from results
    group by year, round, constructor_id
),

-- cumulative sum ordered by round (computed here so the rank below can order by it;
-- T-SQL does not allow a window function inside another window function's ORDER BY)
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
