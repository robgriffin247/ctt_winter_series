with sourced as (
    select * from {{ ref("stg_rounds") }} 
),
gradients_and_dates as (
    select *,
        route_elevation / (route_length*1000) * 100 as route_gradient,
        segment_elevation / (segment_length*1000) * 100 as segment_gradient,
        make_date(year, 1, 4) -- Jan 4th is always in week 1
            + interval (week - 1) weeks 
            - interval (isodow(make_date(year, 1, 4)) - 1) days as week_start,
        make_date(year, 1, 4) 
            + interval (week - 1) weeks 
            - interval (isodow(make_date(year, 1, 4)) - 1) days
            + interval 6 days as week_end
    from {{ref("stg_rounds")}}
),

formatted_dates as (
    select *, 
      case
        -- different years
        when year(week_start) != year(week_end) then
        strftime(week_start, '%b %-d') || 
        case when day(week_start) in (1,21,31) then 'st'
            when day(week_start) in (2,22) then 'nd'
            when day(week_start) in (3,23) then 'rd'
            else 'th' end ||
        ' ' || year(week_start) || ' - ' ||
        strftime(week_end, '%b %-d') ||
        case when day(week_end) in (1,21,31) then 'st'
            when day(week_end) in (2,22) then 'nd'
            when day(week_end) in (3,23) then 'rd'
            else 'th' end ||
        ' ' || year(week_end)
        -- same year, different months
        when month(week_start) != month(week_end) then
        strftime(week_start, '%b %-d') ||
        case when day(week_start) in (1,21,31) then 'st'
            when day(week_start) in (2,22) then 'nd'
            when day(week_start) in (3,23) then 'rd'
            else 'th' end ||
        ' - ' ||
        strftime(week_end, '%b %-d') ||
        case when day(week_end) in (1,21,31) then 'st'
            when day(week_end) in (2,22) then 'nd'
            when day(week_end) in (3,23) then 'rd'
            else 'th' end ||
        ' ' || year(week_end)
        -- same year and month
        else
        strftime(week_start, '%b %-d') ||
        case when day(week_start) in (1,21,31) then 'st'
            when day(week_start) in (2,22) then 'nd'
            when day(week_start) in (3,23) then 'rd'
            else 'th' end ||
        ' - ' ||
        day(week_end) ||
        case when day(week_end) in (1,21,31) then 'st'
            when day(week_end) in (2,22) then 'nd'
            when day(week_end) in (3,23) then 'rd'
            else 'th' end ||
        ' ' || year(week_end)
    end as date_range
    from gradients_and_dates
)

select * from formatted_dates order by round_id