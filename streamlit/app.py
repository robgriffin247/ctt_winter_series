import streamlit as st
import duckdb
import os
import time
from datetime import datetime as dt

if os.getenv("TARGET") == "test":
    DB_PATH = "data/ctt_winter_series_test.duckdb"
elif os.getenv("TARGET") == "dev":
    DB_PATH = f"md:ctt_winter_series_dev"
else:
    DB_PATH = f"md:ctt_winter_series_prod"

st.set_page_config(page_title="CTT Winter Series 2025/26", page_icon=":bike:")

st.html(
    """
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
"""
)


@st.cache_resource(show_spinner="Establishing database connection")
def get_db_connection():
    return duckdb.connect(DB_PATH, read_only=False)


cache_data_days = 0.25


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
    latest_data = con.sql(
        "select /*to_timestamp(max(_dlt_load_id::int))*/ max(_dlt_load_id::int) as x from zrapp.results"
    ).pl()["x"][0]

    return [results, rounds, winners, latest_data]


st.markdown("")
st.title("CTT Winter Series 2025/26")

(
    st.session_state["results"],
    st.session_state["rounds"],
    st.session_state["winners"],
    latest_data,
) = load_data()


results_page = st.Page(
    "pages/public_results.py", title="Results", icon=":material/trophy:"
)
series_analytics_page = st.Page(
    "pages/series_analytics.py", title="Admin", icon=":material/bar_chart:"
)

pg = st.navigation([results_page, series_analytics_page])


st.sidebar.markdown(
    f"Data last updated at {dt.fromtimestamp(latest_data).strftime("%H:%m on %d/%m/%Y")}"
)


pg.run()


# At the end to allow the rest of the page to load - this is just a background process and priority is on ux
if "visit_logged" not in st.session_state:
    st.session_state["visit_logged"] = True

    try:
        con = get_db_connection()

        con.execute(
            """
            INSERT INTO analytics.site_visits (id, timestamp, session_id, app)
            VALUES (gen_random_uuid(), ?, ?, ?)
        """,
            [
                int(time.time()),
                st.runtime.scriptrunner.get_script_run_ctx().session_id,
                os.getenv("APP"),
            ],
        )

    except Exception as e:
        pass
