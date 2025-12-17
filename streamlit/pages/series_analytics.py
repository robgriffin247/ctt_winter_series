import streamlit as st
import duckdb
import os
import polars as pl
import pycountry
import plotly.express as px



def country_name_from_code(code: str) -> str | None:
    if not code:
        return None
    try:
        return pycountry.countries.get(alpha_2=code.upper()).name
    except AttributeError:
        return None

def trend_plot(data, x, y):
    fig = px.line(
        data.to_dict(as_series=False),
        x=x,
        y=y,
    )

    fig.update_layout(
        xaxis_title=x,
        yaxis_title=y,
        yaxis=dict(range=[0, None]),
    )
    st.plotly_chart(fig) 


@st.dialog("Restricted area! Enter the secret code...")
def login():
    password = st.text_input("Password", value=None, type="password", help="Not password123")
    st.session_state["login_state"] = True if password==os.getenv("SERIES_ANALYTICS_SECRET") else False
    if password != os.getenv("SERIES_ANALYTICS_SECRET") and password:
        st.error("Incorrect Password!")
    elif password == os.getenv("SERIES_ANALYTICS_SECRET"):
        st.success("Correct Password!")
    else:
        pass
    if st.session_state["login_state"]:
        st.rerun()


if st.session_state.get("login_state")!=True:
    login()

else:
    results = st.session_state["results"]
    with duckdb.connect() as con:
        n_riders = con.sql("""
                        with source as (select rider_id from results group by 1)
                        select count(*)
                        from source
                        """).pl()[0]
        
        n_efforts = con.sql("""
                        with source as (select rider_id from results)
                        select count(*)
                        from source
                        """).pl()[0]
        
        n_events = con.sql("""
                        with source as (select event_id from results group by 1)
                        select count(*)
                        from source
                        """).pl()[0]
        
        round_stats = con.sql("""
                                select 
                                    round_id,
                                    count(distinct(event_id)) as events,
                                    count(distinct(rider_id)) as riders,
                                    count(*) as efforts,
                                    sum(is_new_pb) as pbs,
                                    sum(route_length) as distance,
                                    sum(route_elevation) as climb,
                                from results 
                                group by 1
                                order by 1""").pl()
        
        round_day_stats = con.sql("""
                                    select 
                                        round_id, 
                                        datetrunc('day', start_datetime_utc) as date,
                                        isodow(start_datetime_utc) as day_number,
                                        dayname(start_datetime_utc) as day,
                                        count(distinct(rider_id)) as riders,
                                        count(*) as efforts,
                                        sum(is_new_pb) as pbs,
                                        sum(route_length) as distance,
                                        sum(route_elevation) as climb,
                                    from results 
                                    group by 1, day(start_datetime_utc), 2, 3, 4
                                    order by 1, day(start_datetime_utc)""").pl()
        
        day_stats = con.sql("""
                                    select 
                                        isodow(start_datetime_utc) as day_number,
                                        dayname(start_datetime_utc) as day,
                                        count(distinct(rider_id)) as riders,
                                        count(*) as efforts,
                                        sum(is_new_pb) as pbs,
                                        sum(route_length) as distance,
                                        sum(route_elevation) as climb,
                                    from results 
                                    group by 1, 2
                                    order by 1""").pl()
        
        event_stats = con.sql("""
                                    select 
                                        round_id,
                                        event_id,
                                        start_datetime_utc,
                                        dayname(start_datetime_utc) as day,
                                        count(distinct(rider_id)) as riders,
                                        count(*) as efforts,
                                        sum(is_new_pb) as pbs,
                                        sum(route_length) as distance,
                                        sum(route_elevation) as climb,
                                    from results 
                                    group by 1, 2, 3
                                    order by 1 desc, start_datetime_utc desc, event_id""").pl()
        
        timeslot_averages = con.sql("""
                                    with 
                                    source as (
                                        select start_datetime_utc 
                                    from results),
                                    
                                    counts_per_timestamp as (
                                        select start_datetime_utc, count(*) as rides 
                                        from source 
                                        group by 1
                                    ),

                                    get_day_and_time as (
                                        select *,
                                            dayname(start_datetime_utc) as day,
                                            strftime(start_datetime_utc, '%H:%M:%S') as time
                                        from counts_per_timestamp
                                    )

                                    select 
                                        day, time, mean(rides) as average_efforts
                                    from get_day_and_time 
                                    group by 1, 2 
                                    order by 1 desc, 2""").pl()
        
        rides_per_country = con.sql("""
                                    select 
                                        country, 
                                        count(*) as efforts,
                                        count(*) / (select count(*) from results) * 100 as percent_of_efforts
                                    from results 
                                    group by 1
                                    order by 2 desc""").pl().with_columns(
                                        pl.col("country")
                                        .map_elements(country_name_from_code, return_dtype=pl.Utf8)
                                        .alias("country_name")
                                    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Riders", n_riders, border=True)
    c2.metric("Efforts", n_efforts, border=True)
    c3.metric("Events", n_events, border=True)

    stats_columns = {
        "round_id":st.column_config.NumberColumn("Round", format="%.0f"),
        "day":st.column_config.TextColumn("Day"),
        "time":st.column_config.TextColumn("Time"),
        "country":st.column_config.TextColumn("Country Code"),
        "country_name":st.column_config.TextColumn("Country"),
        "event_id":st.column_config.NumberColumn("Event", format="%.0f"),
        "start_datetime_utc":st.column_config.DatetimeColumn("Date/Time (UTC)", format="DD-MM-YYYY hh:mm"),
        "events":st.column_config.NumberColumn("Events", format="%.0f"),
        "riders":st.column_config.NumberColumn("Riders", format="%.0f"),
        "efforts":st.column_config.NumberColumn("Efforts", format="%.0f"),
        "percent_of_efforts":st.column_config.NumberColumn("% of Efforts", format="%.1f"),
        "average_efforts":st.column_config.NumberColumn("Efforts", format="%.0f"),
        "pbs":st.column_config.NumberColumn("PBs", format="%.0f"),
        "distance":st.column_config.NumberColumn("Distance", format="%.0f"),
        "climb":st.column_config.NumberColumn("Climb", format="%.0f"),
        }

    with st.expander("Round Stats", expanded=False):
        st.dataframe(round_stats, column_config=stats_columns)
        trend_plot(round_stats, "round_id", "riders")

    with st.expander("Day Stats", expanded=False):
        st.dataframe(day_stats.drop(pl.col("day_number")), column_config=stats_columns)

    with st.expander("Round & Day Stats", expanded=False):
        st.dataframe(round_day_stats.drop(pl.col("day_number", "date")), column_config=stats_columns)
        trend_plot(round_day_stats, "date", "riders")

    with st.expander("Event Stats", expanded=False):
        st.dataframe(event_stats, column_config=stats_columns)

    with st.expander("Timeslot Average Attendances", expanded=False):
        st.dataframe(timeslot_averages, column_config=stats_columns)

    with st.expander("Efforts per Country", expanded=False):
        st.dataframe(rides_per_country[["country_name", "efforts", "percent_of_efforts"]], column_config=stats_columns)


