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

-- sum points per constructor per race, then cumulative sum ordered by round
race_points as (
    select
        year,
        round,
        constructor_id,
        sum(points) as race_points
    from results
    group by year, round, constructor_id
)

select
    rp.year,
    rp.round,
    rp.constructor_id,
    c.constructor_name,
    rp.race_points,
    sum(rp.race_points) over (
        partition by rp.year, rp.constructor_id
        order by rp.round
        rows between unbounded preceding and current row
    ) as cumulative_points,
    rank() over (
        partition by rp.year, rp.round
        order by sum(rp.race_points) over (
            partition by rp.year, rp.constructor_id
            order by rp.round
            rows between unbounded preceding and current row
        ) desc
    ) as championship_position

from race_points rp
left join constructors c using (constructor_id)
