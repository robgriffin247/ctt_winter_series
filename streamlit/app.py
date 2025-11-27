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

standings_tab, best_efforts_tab, race_efforts_tab, stats_tab = st.tabs(["Standings", "Best Efforts", "Race Efforts", "Stats"])

render_standings_tab(standings_tab, standings)
render_best_efforts_tab(best_efforts_tab, round_efforts)
render_race_efforts_tab(race_efforts_tab, results)
render_stats_tab(stats_tab, results)
