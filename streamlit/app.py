import streamlit as st
import duckdb
from tabs import render_standings, render_results, render_stats, render_schedule
import os
import time


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

@st.cache_resource(
    show_spinner="Establishing database connection"
)
def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=False)


cache_data_days = 7

@st.cache_data(
    ttl=cache_data_days * 24 * 60 * 60,
    max_entries=10,
    show_spinner="Loading data from database",
)
def load_data():
    con = get_db_connection()
    results = con.sql("select * from core.obt_results").pl()
    rounds = con.sql("select * from core.fct_rounds").pl()
    winners = con.sql("select * from core.fct_winners").pl()

    return [results, rounds, winners]


st.markdown("")
st.title("CTT Winter Series 2025/26")

if os.getenv("APP")=="modal":
    st.error("Please note, we have moved the results app from modal to [**fly**](https://ctt-winter-series.fly.dev) &mdash; the modal site will not be maintained as routinely as fly, and will shut down in the future, so head over to fly to make sure you stay up to date!")
    time.sleep(5)

standings_tab, results_tab, stats_tab, schedule_tab = st.tabs(
        ["Standings", "Race Efforts", "Stats", "Rounds"]
)

results, rounds, winners = load_data()

with standings_tab:
    render_standings(results)

with results_tab:
    render_results(results)

with stats_tab:
    render_stats(results, christmas=False)

with schedule_tab:
    render_schedule(rounds, winners)


# At the end to allow the rest of the page to load - this is just a background process and priority is on ux
if "visit_logged" not in st.session_state:
    st.session_state["visit_logged"] = True
    
    try:
        con = get_db_connection()

        con.execute("""
            INSERT INTO analytics.site_visits (id, timestamp, session_id, app)
            VALUES (gen_random_uuid(), ?, ?, ?)
        """, [int(time.time()), st.runtime.scriptrunner.get_script_run_ctx().session_id, os.getenv("APP")])

    except Exception as e:
        pass
