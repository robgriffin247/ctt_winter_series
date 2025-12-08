with 

results as (
    select round_id, rider_id, rider, gender, race_seconds, segment_seconds, is_new_pb, mixed_category, womens_category
    from {{ref("obt_results")}} 
    where is_best_effort_in_round and round_completed
),

mens_ranks as (
    select 
        rider_id, rider, round_id, gender, mixed_category as category,
        rank() over (partition by round_id, mixed_category order by race_seconds) as race_rank,
        rank() over (partition by round_id, mixed_category order by segment_seconds) as segment_rank,
        sum(is_new_pb) over (partition by round_id, rider_id) as pb_count,
  
    from results
    where gender='M'
),

womens_ranks as (
    select 
        rider_id, rider, round_id, gender, womens_category as category,
        rank() over (partition by round_id, womens_category order by race_seconds) as race_rank,
        rank() over (partition by round_id, womens_category order by segment_seconds) as segment_rank,
        sum(is_new_pb) over (partition by round_id, rider_id) as pb_count,
    from results
    where gender='F'
),

all_ranks as (
  select * from mens_ranks union all select * from womens_ranks
),

get_scores as (
  select *, race_rank - pb_count + case when segment_rank<=5 then segment_rank-6 else 0 end as round_score
  from all_ranks),

get_round_winners as (
  select *, rank() over (partition by round_id, gender, category order by round_score, race_rank, segment_rank, pb_count)=1 as round_winner from get_scores
)

select * from get_round_winners where round_winner order by round_id, category, gender