with 

source as (
  select * 
  from {{ source("google_sheets", "fts_ranks")}}
),

select_type_and_rename as (
  select
    round_id::int as round_id,
    category::varchar as category,
    rider_id::int as rider_id,
    fts_rank::int as fts_rank,
    notes::varchar as notes,
  from source
)

select * from select_type_and_rename