import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_standings_tab, render_race_efforts_tab, render_best_efforts_tab, render_stats_tab
import os
import plotly.express as px

if os.getenv("TARGET") == "test":
    db = "data/ctt_winter_series_test.duckdb"
if os.getenv("TARGET") == "dev":
    db = f"md:ctt_winter_series_dev"
if os.getenv("TARGET") == "prod":
    db = f"md:ctt_winter_series_prod"

st.set_page_config(#layout="wide",
    page_title="CTT Winter Series 2025/26", page_icon=":bike:"
)



@st.cache_data(
    ttl=6 * 60 * 60,
    max_entries=100,
    show_spinner="Loading data from database...",
)
def load_data():
    with duckdb.connect(db) as con:
        # results = con.sql("select * from core.dim_results").pl()
        # round_results = con.sql("select * from core.dim_round_efforts").pl()
        # standings = con.sql("select * from core.dim_standings").pl()
        results = con.sql("select * from core.obt_results").pl()

    # return [results, round_results, standings, new_results]
    return results

def render_standings(results):
        
    def get_standings(results, selected_gender_category, selected_power_category, selected_age_category, selected_clubs, selected_riders):
        if selected_gender_category=="Mens":
            results = results.filter(pl.col("gender")=="M")
            results = results.with_columns(pl.col("mixed_category").alias("power_category"))

        elif selected_gender_category=="Womens":
            results = results.filter(pl.col("gender")=="F")
            results = results.with_columns(pl.col("womens_category").alias("power_category"))

        else:
            results = results.with_columns(pl.col("mixed_category").alias("power_category"))

        if selected_power_category!="All":
            results = results.filter(pl.col("power_category")==selected_power_category)

        if selected_age_category!="All":
            results = results.filter(pl.col("age_category")==selected_age_category)
    
        if selected_clubs!=[]:
            results = results.filter(pl.col("club").is_in(selected_clubs))
        
        with duckdb.connect() as con:

            standings = con.sql(f"""
                            with 

                            rankings_per_round as (
                                select *,
                                    rank() over (partition by is_best_effort_in_round, round_id order by race_seconds) as race_position,
                                    rank() over (partition by is_best_effort_in_round, round_id order by segment_seconds) as fts_position,
                                    sum(1) over (partition by is_best_effort_in_round, round_id) as of_riders
                            from results
                            where is_best_effort_in_round
                            order by round_id, race_position
                            ),

                            fts_bonuses as (
                            select *,
                                case when fts_position<=5 then fts_position-6 else 0 end as fts_bonus
                            from rankings_per_round    
                            ),

                            round_scores as (
                            select *, race_position + fts_bonus as round_score from fts_bonuses
                            ),

                            rank_rounds_per_rider as (
                                select *,
                                    row_number() over (partition by rider_id, route_type order by round_score) as rider_round_ranking
                                from round_scores
                            ),

                            best_seven_rounds as (
                            select * 
                            from rank_rounds_per_rider
                            where 
                                (route_type='flat' and rider_round_ranking<=4) or 
                                (route_type='rolling' and rider_round_ranking<=2) or 
                                (route_type='mounatinous' and rider_round_ranking<=1) 
                            ),
                            
                            pb_counts as (
                            select rider_id, -count(*) as pb_bonus from results where is_new_pb group by rider_id
                            ),

                            scores_per_rider as (
                            -- Also count races and race per type
                            select
                                best_seven_rounds.rider_id,
                                rider, club, gender, age_category, power_category, country, 
                                sum(1) as qualifying_races,
                                sum(race_position) as position_points,
                                sum(fts_bonus) as fts_bonus,
                                coalesce(last(pb_counts.pb_bonus), 0) as pb_bonus,
                                50 + sum(round_score) + coalesce(last(pb_counts.pb_bonus), 0) as score
                            from best_seven_rounds
                                left join pb_counts using(rider_id)
                            group by best_seven_rounds.rider_id, rider, club, gender, age_category, power_category, country

                            ),
                            
                            
                            filter_riders as (
                            select * from scores_per_rider
                            ),
                                
                            add_rank as (
                                select rank() over (order by qualifying_races desc, score, power_category) as rank, rider, club, power_category, gender, qualifying_races, score, position_points, fts_bonus, pb_bonus,
                                from scores_per_rider  
                            )

                            select * from add_rank
                            order by rank, position_points, fts_bonus, pb_bonus, qualifying_races, rider
                        """).pl()

            if selected_riders!=[]:
                standings = standings.filter(pl.col("rider").is_in(selected_riders))

        return standings

    st.markdown("")
    c1, c2, c3 = st.columns(3)
    c4, c5 = st.columns([2,3])
    st.markdown("")
    st.markdown("")
    gender_category = c1.selectbox("Gender Category", options=["Open", "Mens", "Womens"], key="standings_gender_category")
    power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"], key="standings_power_category")
    age_category = c3.selectbox("Age Category", options=["All", "Jnr", "U23", "Snr", "Mas", "Vet", "50+", "60+", "70+", "80+"], key="standings_age_category")
    clubs = c4.multiselect("Club(s)", options=results[["club", "club_id"]].unique().sort(pl.col("club"))["club"], key="standings_clubs")
    riders = c5.multiselect("Rider(s)", options=results.filter(pl.col("club").is_in(clubs) if len(clubs)>0 else True)[["rider", "rider_id"]].unique().sort(pl.col("rider"))["rider"], key="standings_riders")

    standings = get_standings(results, gender_category, power_category, age_category, clubs, riders)

    if standings.shape[0]==0:
        st.write("Uh oh! No data found &mdash; try a different combination!")
    else:
        st.dataframe(standings,
                    column_config={
                        "rank": st.column_config.NumberColumn("#"),
                        "rider": st.column_config.TextColumn("Rider"),
                        "club": st.column_config.TextColumn("Club"),
                        "gender": st.column_config.TextColumn("Gen."),
                        "power_category": st.column_config.TextColumn("Cat."),
                        "qualifying_races": st.column_config.NumberColumn("Races"),
                        "score": st.column_config.NumberColumn("Points"),
                        "position_points": st.column_config.NumberColumn("Pos."),
                        "fts_bonus": st.column_config.NumberColumn("FTS"),
                        "pb_bonus": st.column_config.NumberColumn("PB"),
                    },
                )

        st.markdown("""
        -----
        The standings table is calculated from each riders best seven qualifying races; the points are calculated dynamically based on the gender, power and age categories, and on the selected clubs. Riders are filtered *after* the leaderboard is calculated &mdash; search for a rider and you will see their position given the combination of categories and clubs, perfect for a bit of internal club competition 😉 

        - *#* = rank within selected power and gender category; sorted on races completed, then points
        - *Cat* = power category A-D
        - *Gen* = gender (self-reported male or female)
        - *Races* = number of rounds completed and contributing to score (up to 7)
        - *Points* = total points = 50 + positions - fts points - pb points
        - *Pos.* = position points (sum of positions from contributing rounds)
        - *FTS* = total fts points from contributing rounds
        - *PB* = total PBs **in whole series**
        """)

def render_results(results):
    st.markdown("")
    c4, c5 = st.columns([2,3])
    st.markdown("")
    c6, c7 = st.columns([3,2])
    st.markdown("")
    st.markdown("")

    clubs = c4.multiselect("Club(s)", options=results[["club", "club_id"]].unique().sort(pl.col("club"))["club"], key="results_clubs")
    if clubs!=[]:
        results = results.filter(pl.col("club").is_in(clubs))

    riders = c5.multiselect("Rider(s)", options=results[["rider", "rider_id"]].unique().sort(pl.col("rider"))["rider"], key="results_riders")
    if riders!=[]:
        results = results.filter(pl.col("rider").is_in(riders))

    routes = c6.multiselect("Route(s)", options=results[["route"]].unique().sort(pl.col("route"))["route"], key="results_route")
    if routes!=[]:
        results = results.filter(pl.col("route").is_in(routes))

    rounds = c7.multiselect("Round(s)", options=results[["round_id"]].unique().sort(pl.col("round_id"))["round_id"], key="results_round")
    if rounds!=[]:
        results = results.filter(pl.col("round_id").is_in(rounds))



    if results.shape[0]==0:
        st.write("Uh oh! No data found &mdash; try a different combination!")
    else:
        st.dataframe(results[["round_id", "rider", "club", "country", "gender", "age_category", "categories", "watts_average", "wkg_average", "race_time", "race_speed", "is_best_effort_in_round", "is_new_pb", "segment_time", "start_datetime_utc", "route"]].sort([pl.col("round_id"), pl.col("start_datetime_utc"), pl.col("race_speed")], descending=[False, False, True]),
                    column_config={
                        "round_id":st.column_config.NumberColumn("Rnd", pinned=True),
                        "rider":st.column_config.TextColumn("Rider", pinned=True),
                        "club":st.column_config.TextColumn("Club"),
                        "country":st.column_config.TextColumn("Country"),
                        "gender":st.column_config.TextColumn("Gender"),
                        "age_category":st.column_config.TextColumn("Age"),
                        "categories":st.column_config.TextColumn("Category"),
                        #"mixed_category":st.column_config.TextColumn("Cat."),
                        #"womens_category":st.column_config.TextColumn("(W)"),
                        "watts_average":st.column_config.NumberColumn("Watts", format="%.0f"),
                        "wkg_average":st.column_config.NumberColumn("W/kg", format="%0.1f"),
                        "race_time":st.column_config.TextColumn("Time"),
                        #"race_seconds":st.column_config.TextColumn("Time"),
                        "race_speed":st.column_config.NumberColumn("Speed (km/h)", format="%.2f"),
                        "is_best_effort_in_round":st.column_config.CheckboxColumn("RB"),
                        "is_new_pb":st.column_config.CheckboxColumn("PB"),
                        "segment_time":st.column_config.TextColumn("Segment"),
                        #"segment_seconds":st.column_config.TextColumn("Segment"),
                        "start_datetime_utc":st.column_config.DatetimeColumn("Date/Time", format="localized"),
                        "route":st.column_config.TextColumn("Route"),
                        "route_type":st.column_config.TextColumn("Type"),
                    })

        if len(riders)==1:
            c1, c2, c3 = st.columns(3)
            c1.metric("Flat", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='flat').shape[0]} of 4", border=True)
            c2.metric("Rolling", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='rolling').shape[0]} of 2", border=True)
            c3.metric("Mountainous", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='mountainous').shape[0]} of 1", border=True)

        st.markdown("""
                    -----
                    This table shows every effort from across the series; you can find your round-bests (RB; the fastest attempts per round, those that determine your score for the round).
                    """)

def render_stats(results):

    def power_figure(data):

        c1, c2 = st.columns([6,3])
        c2.markdown("")
        c2.markdown("")
        x_metric = c2.selectbox("X-Axis Value", options=["W/kg", "Watts",])
        y_metric = c2.selectbox("Y-Axis Value", options=["Speed", "Time",])
        selected_routes = c2.multiselect("Route", options=data[["route"]].unique().sort(pl.col("route"))["route"].to_list())
        selected_power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"])

        fig = px.scatter(
            data
                .filter(pl.col("route").is_in(selected_routes) if len(selected_routes)>0 else True)
                .filter(pl.col("mixed_category")==selected_power_category if selected_power_category!="All" else True)
                .sort(pl.col("mixed_category")),
            x="watts_average" if x_metric=="Watts" else "wkg_average",
            y="race_seconds" if y_metric=="Time" else "race_speed",
            color="mixed_category",
            opacity=0.4,
            labels={
                "watts_average": "Average Power (W)",
                "wkg_average": "Average Power (W/kg)",
                "race_seconds": "Time (seconds)",
                "race_speed": "Speed (km/h)",
                "mixed_category": "Category"
            },
            )
        
        c1.plotly_chart(fig)

    distance = sum(results["route_length"])
    hours = sum(results["race_seconds"])/3600
    kwhs = sum((results["watts_average"]/1000) * (results["race_seconds"]/3600))
        
    c1, c2, c3 = st.columns(3)
    c1.metric("Riders 🚴‍♂️", f"{results[["rider_id"]].unique().shape[0]:,.0f}", border=True)
    c2.metric("Efforts 🏁", f"{results.shape[0]:,.0f}", border=True)
    c3.metric("PBs 🏆", f"{sum(results["is_new_pb"]):,.0f}", border=True)
    
    c1.metric("Distance 🌍", f"{distance:,.0f} km", border=True)
    c2.metric("Hours ⏱️", f"{hours:,.0f}", border=True)
    #c3.metric("Speed 🚀", f"{distance/hours:,.1f} km/h", border=True)
    #c3.metric("Elevation 🏔️", f"{sum(summaries_data["route_elevation"]):,.0f} m", border=True)
    c3.metric("Everests Climbed 🏔️", f"{sum(results["route_elevation"])/8848:,.2f}", border=True)
    
    c1.metric("Energy Generated ⚡", f"{kwhs:,.0f} kW/h", border=True)
    c2.metric("Calories Burned 🔥", f"{kwhs * 860.420 / 0.24:,.0f}", border=True)
    c3.metric("Pizza Slices 🍕", f"{kwhs * 860.420 / 0.24 / 266:,.0f}", border=True)
    
    power_figure(results)




st.markdown("")

st.title("CTT Winter Series 2025/26")


standings_tab, results_tab, stats_tab = st.tabs(["Standings", "Race Efforts", "Stats"])

results = load_data()

with standings_tab:
    render_standings(results)

with results_tab:
    render_results(results)

with stats_tab:
    render_stats(results)



# Add breakdown; of countries, ages, genders, categories
# Add round calendar; needs dbt model and week numbers in raw
# Add signup link https://zwiftinc.sjv.io/c/2607378/1772639/20902?u=https%3A%2F%2Fwww.zwift.com%2Fevents%2Ftag%2Fcyclingtimetrials