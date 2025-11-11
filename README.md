# ctt_winter_series

### Setup

1. Clone the repo

    ```
    git clone git@github.com:robgriffin247/ctt_winter_series
    cd ctt_winter_series
    uv init
    ```

1. Add .gitignore:

    ```
    .env
    ```

1. Add .env (replace ``<api_key>``):

    ```
    TARGET="dev"
    ZRAPP_API_KEY="<api_key>"
    ```

1. Add .envrc

    ```
    # Handle windows carriage-returns
    sed -i 's/\r$//' .env

    # Export .env variables
    set -a
    source .env
    set +a
    ```

1. Enable direnv

    ```
    direnv allow
    ```

1. Add Python packages

    ```
    uv add httpx duckdb polars dlt dlt[duckdb] dlt[parquet] dbt-core dbt-duckdb
    ```

1. Add code for ingestion and transformations

    - ingestion/
    - models/
    - macros/
    - dbt_project.yml
    - profiles.yml

1. Set up MotherDuck account and token

1. Get results with ``uv run python3 ingestion/zrapp.py <id>``

1. Transform results with ``uv run dbt build`` 

- Where ingestion and transformation occurs depends on the value of TARGET in the working environment; I have setup to have three databases - two local (test and dev) and prod on MotherDuck