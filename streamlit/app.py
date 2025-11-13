import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_leaderboard_tab

st.set_page_config(#layout="wide",
                   page_title="CTT Winter Series 2025/26",
                   page_icon=":bike:")

st.title("CTT Winter Series 2025/26")

def load_data():
    with duckdb.connect("data/ctt_winter_series_test.duckdb") as con:
        event_results = con.sql("select * from core.dim_event_results").pl()
        leaderboard = con.sql("select * from core.dim_leaderboard").pl()

    return [event_results, leaderboard]

event_results, leaderboard = load_data()

leaderboard_tab, event_results_tab, stats_tab, calendar_tab, rules_tab = st.tabs(["Leaderboard", "Race Results", "Statistics", "Calendar", "Rules"])

render_leaderboard_tab(leaderboard_tab, leaderboard)

with rules_tab:
    st.markdown("*Position Points = Sum of best seven positions from four flat, two rolling and one mountain race*")
