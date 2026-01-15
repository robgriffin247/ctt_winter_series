with 
source as (
    select
        zid::int as event_id,
        zwid::int as rider_id,

        msec___35::decimal as msec___35,
        watts___35::decimal as watts___35,
        wkg___35::decimal as wkg___35,

        msec___116::decimal as msec___116,
        watts___116::decimal as watts___116,
        wkg___116::decimal as wkg___116,

        msec___1::decimal as msec___1,
        watts___1::decimal as watts___1,
        wkg___1::decimal as wkg___1,
        
        msec___20::decimal as msec___20,
        watts___20::decimal as watts___20,
        wkg___20::decimal as wkg___20,
        
        msec___3::decimal as msec___3,
        watts___3::decimal as watts___3,
        wkg___3::decimal as wkg___3,
        
    from {{source("zpdatafetch", "sprint_results")}}
)

select * from source