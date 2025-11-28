import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_standings_tab, render_race_efforts_tab, render_best_efforts_tab, render_stats_tab
import os

if os.getenv("TARGET") == "test":
    db = "data/ctt_winter_series_test.duckdb"
if os.getenv("TARGET") == "dev":
    db = f"md:ctt_winter_series_dev"
if os.getenv("TARGET") == "prod":
    db = f"md:ctt_winter_series_prod"

st.set_page_config(  # layout="wide",
    page_title="CTT Winter Series 2025/26", page_icon=":bike:"
)

st.title("CTT Winter Series 2025/26")


@st.cache_data(
    ttl=6 * 60 * 60,
    max_entries=100,
    show_spinner="Loading data from database...",
)
def load_data():
    with duckdb.connect(db) as con:
        results = con.sql("select * from core.dim_results").pl()
        round_results = con.sql("select * from core.dim_round_efforts").pl()
        standings = con.sql("select * from core.dim_standings").pl()

    return [results, round_results, standings]

results, round_efforts, standings = load_data()




def get_standings(results, round_efforts):
    with duckdb.connect() as con:
        standings = con.sql("""
        with 
                        
        derive_fts_bonus as (
            select *,
                case when segment_rank <=5 then segment_rank-6 else 0 end as fts_bonus
            from round_efforts
        ),

        get_points_per_round as (
            select *,
                race_rank + fts_bonus as score
            from derive_fts_bonus
        ),
        
        -- riders best rounds
        --    ranking within rider and gender category (as there are two gender_categories)
        add_rider_round_ranking as (
            select *,
                row_number() over (partition by rider_id, gender_category order by score) as rider_round_ranking
            from get_points_per_round
        ),

        get_best_seven as (
        select * exclude(route_type, rider_round_ranking)
        from add_rider_round_ranking
        where 
            (rider_round_ranking<=4 and route_type='flat')
            or (rider_round_ranking<=4 and route_type='rolling')
            or (rider_round_ranking<=1 and route_type='mountain')
        ),

        -- and sum the scores to one per rider/gender_category
        total_scores as (
            select 
                rider_id,
                rider,
                club_id,
                club,
                gender_category,
                power_category,
                age_category,
                country,
                sum(1) as race_count,
                sum(race_rank) as position_points,
                sum(fts_bonus) as segment_bonuses,
                sum(score) as score    
            from get_best_seven
            group by all
        ),

        -- then add in the pb bonuses per rider
        pb_counts as (
            select rider_id, sum(new_pb) as pb_count 
            from results
            where gender_category='Mixed'
            group by rider_id
        ),

        -- add pb bonus and get the final score
        add_pb_bonus as (
            select 
                total_scores.* exclude(score),
                - pb_counts.pb_count as pb_bonuses,
                50 + score - pb_counts.pb_count as score,
            from total_scores left join pb_counts using(rider_id)
        ),

        add_rank as (
            select *, 
                rank() over (partition by power_category, gender_category order by race_count desc, score, pb_bonuses, segment_bonuses) as rank
            from add_pb_bonus
        )
        select * from get_best_seven
        --select * from add_rank order by race_count desc, power_category, gender_category, rank                    
        """).pl()

    return standings



st.dataframe(
    get_standings(
        results
            .filter(pl.col("gender_category")=="Womens")
            .filter(pl.col("power_category")=="B"), 
        round_efforts)
            .filter(pl.col("gender_category")=="Womens")
            .filter(pl.col("power_category")=="B")
)





standings_tab, best_efforts_tab, race_efforts_tab, stats_tab = st.tabs(["Standings", "Best Efforts", "Race Efforts", "Stats"])

render_standings_tab(standings_tab, standings)
render_best_efforts_tab(best_efforts_tab, round_efforts)
render_race_efforts_tab(race_efforts_tab, results)
render_stats_tab(stats_tab, results)
