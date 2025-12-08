import duckdb
import os


def ingest_sheets():

    target = os.getenv("TARGET")

    motherduck_token = os.getenv("MOTHERDUCK_TOKEN")
    if target == "prod":
        database = f"md:ctt_winter_series_prod?motherduck_token={motherduck_token}"
    if target == "dev":
        database = f"md:ctt_winter_series_dev?motherduck_token={motherduck_token}"
    if target == "test":
        database = "data/ctt_winter_series_test.duckdb"

    def get_sheet(sheet):
        with duckdb.connect(database) as con:
            con.execute(
                f"""
                create schema if not exists google_sheets;

                create or replace table google_sheets.{sheet} as
                select * 
                from read_csv_auto('https://docs.google.com/spreadsheets/d/{os.getenv("GOOGLE_SHEET_ID")}/export?format=csv&gid={os.getenv(f"GOOGLE_SHEET_{sheet.upper()}")}');
            """
            )

            print(f"Loaded {sheet.upper()} from Google Sheets")

    for s in ["rounds", "events"]:#, "segment_times"]:
        get_sheet(s)


if __name__ == "__main__":
    ingest_sheets()
