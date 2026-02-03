import duckdb
import os


def ingest_sheets():

    target = os.getenv("TARGET")
    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")

    if target in ["prod", "dev"]:
        database = f"md:ctt_winter_series_{target}?motherduck_token={motherduck_token}"
    if target == "test":
        database = "data/ctt_winter_series_test.duckdb"

    def get_rounds():
        with duckdb.connect(database) as con:
            con.execute(
                f"""
                create schema if not exists google_sheets;

                create or replace table google_sheets.rounds as
                select * 
                from read_csv_auto('https://docs.google.com/spreadsheets/d/{os.getenv("GOOGLE_SHEET_ID")}/export?format=csv&gid={os.getenv("GOOGLE_SHEET_ROUNDS_SHEET_ID")}');
            """
            )

            print("Loaded Rounds from Google Sheets")

    def get_races():
        with duckdb.connect(database) as con:
            con.execute(
                f"""
                create schema if not exists google_sheets;

                create or replace table google_sheets.races as
                select * 
                from read_csv_auto('https://docs.google.com/spreadsheets/d/{os.getenv("GOOGLE_SHEET_ID")}/export?format=csv&gid={os.getenv("GOOGLE_SHEET_RACES_SHEET_ID")}');
            """
            )

            print("Loaded Races from Google Sheets")

    get_rounds()
    get_races()


if __name__ == "__main__":
    ingest_sheets()
