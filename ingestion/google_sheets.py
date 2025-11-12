import duckdb
import os


def ingest_sheets():
    
    target = os.getenv("TARGET")

    if target == "prod":
        motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
        database = f"md:ctt_winter_series_prod?motherduck_token={motherduck_token}"
    if target=="dev":
        database = "data/ctt_winter_series_dev.duckdb"
    if target=="test":
        database = "data/ctt_winter_series_test.duckdb"

    def get_events():
        with duckdb.connect(database) as con:
            con.execute(f"""
                create schema if not exists google_sheets;

                create or replace table google_sheets.events as
                select * 
                from read_csv_auto('https://docs.google.com/spreadsheets/d/{os.getenv("GOOGLE_SHEET_ID")}/export?format=csv&gid={os.getenv("GOOGLE_SHEET_EVENTS")}');
            """)

            print("Loaded Events from Google Sheets")
        
    def get_points_modifiers():
        with duckdb.connect(database) as con:
            con.execute(f"""
                create schema if not exists google_sheets;

                create or replace table google_sheets.points_modifiers as
                select * 
                from read_csv_auto('https://docs.google.com/spreadsheets/d/{os.getenv("GOOGLE_SHEET_ID")}/export?format=csv&gid={os.getenv("GOOGLE_SHEET_POINTS_MODIFIERS")}');
            """)

            print("Loaded Points Modifiers from Google Sheets")
        
    get_events()
    get_points_modifiers()


if __name__=="__main__":
    ingest_sheets()