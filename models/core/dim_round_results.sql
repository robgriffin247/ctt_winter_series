with results as (select * from {{ ref("int_round_results") }})

select * from results