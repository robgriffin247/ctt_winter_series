with source as (select * from {{source("zpdatafetch", "sprint_results")}})

select * from source