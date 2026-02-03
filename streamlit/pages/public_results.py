import streamlit as st
import os
import time
from tabs import render_standings, render_results, render_stats, render_schedule

if os.getenv("APP") == "modal":
    st.error(
        "Please note, we have moved the results app from modal to [**fly**](https://ctt-winter-series.fly.dev) &mdash; the modal site will not be maintained as routinely as fly, and will shut down in the future, so head over to fly to make sure you stay up to date!"
    )
    time.sleep(5)

standings_tab, results_tab, stats_tab, schedule_tab = st.tabs(
    ["Standings", "Race Efforts", "Stats", "Rounds"]
)

results = st.session_state["results"]
rounds = st.session_state["rounds"]
winners = st.session_state["winners"]

with standings_tab:
    render_standings(results)

with results_tab:
    render_results(results)

with stats_tab:
    render_stats(results, christmas=False)

with schedule_tab:
    render_schedule(rounds, winners)
