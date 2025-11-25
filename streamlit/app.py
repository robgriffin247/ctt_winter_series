import streamlit as st
import duckdb
import polars as pl
import html
from tabs import render_standings_tab, render_race_results_tab
import os
import plotly.express as px

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

# standings_tab, results_tab, stats_tab, calendar_tab, rules_tab = st.tabs(["Standings", "Race Results", "Statistics", "Calendar", "Rules"])
standings_tab, power_plots_tab = st.tabs(["Standings", "Power Plots"])
#standings_tab = st.container()

render_standings_tab(standings_tab, standings)

#render_race_results_tab(race_results_tab, results)
# with rules_tab:
#     st.markdown("*Position Points = Sum of best seven positions from four flat, two rolling and one mountain race*")


def power_figure():

    c1, c2 = st.columns([6,3])
    c2.markdown("")
    c2.markdown("")
    x_metric = c2.selectbox("X-Axis Value", options=["W/kg", "Watts",])
    y_metric = c2.selectbox("Y-Axis Value", options=["Speed", "Time",])
    selected_routes = c2.multiselect("Route", options=results[["route"]].unique().sort(pl.col("route"))["route"].to_list())
    selected_gender_category = c2.selectbox("Gender Category", options=["Mixed", "Womens", "Mens"])
    selected_power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"])

    fig = px.scatter(
        results
            .filter(pl.col("route").is_in(selected_routes) if len(selected_routes)>0 else True)
            .filter(pl.col("gender_category")==selected_gender_category if len(selected_gender_category)>0 else True)
            .filter(pl.col("power_category")==selected_power_category if selected_power_category!="All" else True)
            .sort(pl.col("power_category")),
        x="watts_average" if x_metric=="Watts" else "wkg_average",
        y="race_seconds" if y_metric=="Time" else "race_speed",
        color="power_category",
        opacity=0.4,
        labels={
            "watts_average": "Average Power (W)",
            "wkg_average": "Average Power (W/kg)",
            "race_seconds": "Time (seconds)",
            "race_speed": "Speed (km/h)",
            "power_category": "Power Category"
        },
        )
    
    c1.plotly_chart(fig)



with power_plots_tab:
    power_figure()