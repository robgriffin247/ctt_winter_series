import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_standings_tab, render_race_results_tab
import os

st.set_page_config(#layout="wide",
                   page_title="CTT Winter Series 2025/26",
                   page_icon=":bike:")

st.title("CTT Winter Series 2025/26")


@st.cache_data(
    ttl=6 * 60 * 60,
    max_entries=100,
    show_spinner="Loading data from database...",
)
def load_data():
    with duckdb.connect(f"md:ctt_winter_series_prod") as con:    
        results = con.sql("select * from core.dim_results").pl()
        round_results = con.sql("select * from core.dim_round_results").pl()
        standings = con.sql("select * from core.dim_standings").pl()

    return [results, round_results, standings]

results, round_results, standings = load_data()

# standings_tab, results_tab, stats_tab, calendar_tab, rules_tab = st.tabs(["Standings", "Race Results", "Statistics", "Calendar", "Rules"])
# standings_tab, race_results_tab = st.tabs(["Standings", "Race Results"])
standings_tab = st.container()

render_standings_tab(standings_tab, standings)

# render_race_results_tab(race_results_tab, results)
# with rules_tab:
#     st.markdown("*Position Points = Sum of best seven positions from four flat, two rolling and one mountain race*")
