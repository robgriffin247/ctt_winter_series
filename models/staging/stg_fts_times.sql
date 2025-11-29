with 
source as (
    select
        zid::int as event_id,
        zwid::int as rider_id,
        msec___35::decimal as msec___35,
        msec___116::decimal as msec___116,
        msec___1::decimal as msec___1
    from {{source("zpdatafetch", "sprint_results")}}
)

select * from source