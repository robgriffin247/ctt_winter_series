import streamlit as st
import duckdb
from tabs import render_standings, render_results, render_stats, render_schedule
import os

if os.getenv("TARGET") == "test":
    DB_PATH = "data/ctt_winter_series_test.duckdb"
elif os.getenv("TARGET") == "dev":
    DB_PATH = f"md:ctt_winter_series_dev"
else:
    DB_PATH = f"md:ctt_winter_series_prod"

st.set_page_config(
    page_title="CTT Winter Series 2025/26", 
    page_icon=":bike:"
)

st.html("""
<style>
        .stMainBlockContainer, stVerticalBlock {
            width: 95% !important;
            max-width: 950px !important;
        }

        a{
            color: #fc6719 !important; 
            text-decoration: none !important;
        }
        
        .stMetric {
            background-color: #fc671910;  # Light orange background
            padding: 15px;
            border-radius: 5px;
        }
        
        .stMetric:hover {
            background-color: #fc671920;  # Light orange background
            padding: 15px;
            border-radius: 5px;
        }
</style>
""")

@st.cache_resource
def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=True)

@st.cache_data(
    ttl=4 * 60 * 60,
    max_entries=10,
    show_spinner="Loading data from database...",
)
def load_data():
    con = get_db_connection()
    results = con.sql("select * from core.obt_results").pl()
    rounds = con.sql("select * from core.fct_rounds").pl()
    winners = con.sql("select * from core.fct_winners").pl()

    return [results, rounds, winners]



st.markdown("")
st.title("CTT Winter Series 2025/26")

standings_tab, results_tab, stats_tab, schedule_tab = st.tabs(
        ["Standings", "Race Efforts", "Stats", "Rounds"]
)

results, rounds, winners = load_data()

with standings_tab:
    render_standings(results)

with results_tab:
    render_results(results)

with stats_tab:
    render_stats(results)

with schedule_tab:
    render_schedule(rounds, winners)

