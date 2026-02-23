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
            # this is all past races; excludes future races and highlights recent races
            races as (
                select 
                    event_id,
                    datediff('hours', start_datetime_utc, now())<48 as is_recent
                from google_sheets.races
                where 
                    datediff('minutes', start_datetime_utc, now())>0
            ),

            ingested_races as (
                select 
                    event_id
                from zrapp.results
                group by all
            )

            # Take any past race not already loaded, as well as reloading any recent races
            select * from races where event_id not in (select event_id from ingested_races) or is_recent
            """
            )
            .pl()["event_id"]
            .to_list()
        )

    if len(races_to_load) > 0:
        i = 0
        for race in races_to_load:
            print("="*50)
            print(f"Loading race {race} ({i+1} of {len(races_to_load)})")
            if i!=0:
                print(f"⏳ Waiting for 70 seconds before loading")
                time.sleep(70)
            i += 1
            ingest_zrapp(race)
            ingest_zpdatafetch(race)
            print(f"✅ Loaded race {race}")

    run_dbt_transformations()

    return f"ETL job complete, loaded {len(races_to_load)} races!"


if __name__ == "__main__":
    print(etl_job())
