with 
rounds as (select * from {{ref("int_rounds")}}),
winners as (select * from {{ref("fct_winners")}}),

prep_agg as (
  select round_id, rider, case when gender='M' then 'Mens' else 'Womens' end as gender, category from winners
), 

grouped as (
  select 
    round_id,
    gender,
    category,
    string_agg(rider, ', ' order by rider) as riders
  from prep_agg
  group by round_id, gender, category
),
  
aggregated as (
  select 
  round_id,
  string_agg(
    case when gender = 'Mens' then category || ': ' || riders end,
    '; '
    order by category
  ) as mens,
  string_agg(
    case when gender = 'Womens' then category || ': ' || riders end,
    '; '
    order by category
  ) as womens
from grouped
group by round_id
order by round_id)

select rounds.*, aggregated.mens, aggregated.womens from rounds left join aggregated using(round_id) order by round_id