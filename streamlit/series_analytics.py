import streamlit as st
import os 
import duckdb 

if os.getenv("TARGET") == "test":
    DB_PATH = "data/ctt_winter_series_test.duckdb"
elif os.getenv("TARGET") == "dev":
    DB_PATH = f"md:ctt_winter_series_dev"
else:
    DB_PATH = f"md:ctt_winter_series_prod"

st.set_page_config(
    page_title="CTT Winter Series 2025/26 - Series Analytics", 
    page_icon=":bike:"
)


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
    site_visits = con.sql("select * from analytics.site_visits").pl()

    return [results, site_visits]

results, site_visits = load_data()

st.dataframe(results)
st.dataframe(site_visits)


def site_traffic():
    with duckdb.connect() as con:
        daily_counts = con.sql("""
        select
            date(to_timestamp(timestamp)) as date,
            count(*) as visits
        from site_visits
        group by 1
        """).pl()

        daily_counts_by_app = con.sql("""
        select
            date(to_timestamp(timestamp)) as date,
            app,
            count(*) as visits
        from site_visits
        group by 1, 2
        """).pl()

    st.dataframe(daily_counts)
    st.dataframe(daily_counts_by_app)

site_traffic()