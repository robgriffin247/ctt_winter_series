import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_standings_tab, render_race_results_tab

st.set_page_config(#layout="wide",
                   page_title="CTT Winter Series 2025/26",
                   page_icon=":bike:")

st.title("CTT Winter Series 2025/26")

def load_data():
    with duckdb.connect("data/ctt_winter_series_test.duckdb") as con:
        results = con.sql("select * from core.dim_results").pl()
        standings = con.sql("select * from core.dim_standings").pl()

    return [results, standings]

results, standings = load_data()

# standings_tab, results_tab, stats_tab, calendar_tab, rules_tab = st.tabs(["Standings", "Race Results", "Statistics", "Calendar", "Rules"])
standings_tab, race_results_tab = st.tabs(["Standings", "Race Results"])

render_standings_tab(standings_tab, standings)

render_race_results_tab(race_results_tab, results)
# with rules_tab:
#     st.markdown("*Position Points = Sum of best seven positions from four flat, two rolling and one mountain race*")
