-- NEED TO GUARD AGAINST MULTIPLE LOADS OF SAME RACE

with 
event_results as (
  select * 
  from {{ ref("int_event_results") }}
), 

rounds as (
  select *
  from {{ ref("stg_rounds") }}
),

events as (
  select *
  from {{ ref("stg_events") }}
),

fts_ranks as (
  select * 
  from {{ ref("int_fts_ranks") }}
),

riders as (
  select
    rider_id,
    rider, 
    club,
    gender,
    category
  from event_results
  group by all
),

-- start time needed for PB, round_id needed to get best pos per round
add_event_details as (
  select event_results.*, events.round_id, events.start_datetime_utc
  from event_results left join events using(event_id)
),


-- Get best position per round
round_best_positions as (
  select 
    round_id, 
    rider_id,
    min(position) as best_position,
    min(category_position) as best_category_position,
    min(gender_position) as best_gender_position,
    min(category_gender_position) as best_category_gender_position
  from add_event_details
  group by 1, 2
  order by round_id
),


-- get FTS per round
fts_points as (
  select
    round_id, 
    rider_id,
    case round_rank 
      when 5 then 0 -- -1
      when 4 then 0 -- -2
      when 3 then 0 -- -3
      when 2 then 0 -- -4
      when 1 then 0 -- -5
      else 0 end as fts_bonus
  from fts_ranks
),



-- get number of PBs per round
add_route_to_event_results as (
  select add_event_details.*, rounds.route
  from add_event_details left join rounds using(round_id)
),

add_rolling_pb as (
  select *,
    min(time_seconds) over (partition by rider_id, route order by start_datetime_utc) as rolling_pb
  from add_route_to_event_results
),

add_new_pb as (
  select *,
    rolling_pb - lag(rolling_pb) over (partition by rider_id, route order by start_datetime_utc) as pb_change
  from add_rolling_pb
),

new_pb as (
  select *, 
    case when pb_change<0 then 1 else 0 end as pb_points
  from add_new_pb
),

new_pbs_per_round as (
  select round_id, rider_id, -sum(pb_points) as pb_bonus from new_pb group by 1,2
),


-- Get it all added up
combine_all_points as (
  select
    round_best_positions.*,
    coalesce(fts_points.fts_bonus, 0) as fts_bonus,
    coalesce(new_pbs_per_round.pb_bonus, 0) as pb_bonus,
    round_best_positions.best_category_position + coalesce(new_pbs_per_round.pb_bonus, 0) + coalesce(fts_points.fts_bonus, 0) as round_points
  from round_best_positions
    left join fts_points using(round_id, rider_id)
    left join new_pbs_per_round using(round_id, rider_id)
),

add_type as (
  select combine_all_points.*, rounds.type
  from combine_all_points 
    left join rounds using(round_id)
),

add_row_number as (
  select *, row_number() over (partition by rider_id, type order by round_points) as rider_round_points_rank_by_type
  from add_type
),

point_scoring_rounds as (
  select * from add_row_number
  where 
    (type='flat' and rider_round_points_rank_by_type <= 4)
    or (type='rolling' and rider_round_points_rank_by_type <= 2)
    or (type='mountain' and rider_round_points_rank_by_type = 1)
),

sum_position_points as (
  select 
    rider_id, 
    sum(best_category_position) as position_points, 
    sum(fts_bonus) as fts_bonus, 
    sum(pb_bonus) as pb_bonus, 
    sum(round_points) + 50 as points,
  from point_scoring_rounds
  group by rider_id
),

add_to_rider_details as (
  select 
    riders.*,
    sum_position_points.* exclude(rider_id),
    row_number() over (partition by category order by points) as category_rank,
  from riders left join sum_position_points using(rider_id)
),

select_cols as (
  select
    rider_id,
    category, 
    category_rank,
    rider,
    club,
    points,
    position_points,
    fts_bonus,
    pb_bonus
  from add_to_rider_details
  order by category, points
)

select * from select_cols 



    -- case 
    --   when gap_seconds = 0 then null
    --   when gap_seconds < 3600 then 
    --       lpad(floor(gap_seconds / 60)::integer::varchar, 2, '0') || ':' ||
    --       lpad(printf('%.3f', gap_seconds % 60), 6, '0')
    --   else 
    --       lpad(floor(gap_seconds / 3600)::integer::varchar, 2, '0') || ':' ||
    --       lpad(floor((gap_seconds % 3600) / 60)::integer::varchar, 2, '0') || ':' ||
    --       lpad(printf('%.3f', gap_seconds % 60), 6, '0')
    -- end as gap_formatted