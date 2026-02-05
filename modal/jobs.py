import duckdb
import os
import time

from ingestion.google_sheets import ingest_sheets
from ingestion.zpdf import ingest_zpdatafetch
from ingestion.zrapp import ingest_zrapp
from transformer import run_dbt_transformations

if os.getenv("TARGET") == "test":
    DB_PATH = "data/ctt_winter_series_test.duckdb"
elif os.getenv("TARGET") == "dev":
    DB_PATH = f"md:ctt_winter_series_dev"
else:
    DB_PATH = f"md:ctt_winter_series_prod"


def etl_job():
    ingest_sheets()

    with duckdb.connect(DB_PATH) as con:
        races_to_load = (
            con.sql(
                """
            with 
            races as (
                select 
                    event_id,
                    datediff('hours', start_datetime_utc, now())<=(2*24) as is_recent
                from google_sheets.races
            ),

            ingested_races as (
                select 
                    event_id
                from zrapp.results
                group by all
            )

            select * from races where event_id not in (select event_id from ingested_races) or is_recent
            """
            )
            .pl()["event_id"]
            .to_list()
        )

    if len(races_to_load) > 0:
        i = 0
        for race in races_to_load:
            if i!=0:
                print(f"Waiting for 70 seconds before loading {race}")
                time.sleep(70)
            i += 1
            ingest_zrapp(race)
            ingest_zpdatafetch(race)

    run_dbt_transformations()

    return f"ETL job complete, loaded {len(races_to_load)} races!"


if __name__ == "__main__":
    print(etl_job())
