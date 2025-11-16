with 

standings as (
    select
        *
    from {{ ref("int_standings") }}
    order by overall_score
)

select * from standings
