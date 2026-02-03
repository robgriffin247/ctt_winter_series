from jobs import etl_job

import modal
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent

image = (
    modal.Image.debian_slim()
    # .pip_install("dlt[motherduck]", "dbt-duckdb", "httpx", "keyring", "zpdatafetch", "polars")
    .pip_install_from_pyproject(
        PROJECT_ROOT / "pyproject.toml",
    )
    # modal code
    .add_local_file(PROJECT_ROOT / "modal/jobs.py", "/root/jobs.py")
    .add_local_file(PROJECT_ROOT / "modal/transformer.py", "/root/transformer.py")
    # ingestion code
    .add_local_dir(PROJECT_ROOT / "ingestion", "/root/ingestion")
    # dbt folders
    .add_local_dir(PROJECT_ROOT / "models", "/root/models")
    .add_local_dir(PROJECT_ROOT / "seeds", "/root/seeds")
    # dbt files
    .add_local_file(PROJECT_ROOT / "dbt_project.yml", "/root/dbt_project.yml")
    .add_local_file(PROJECT_ROOT / "profiles.yml", "/root/profiles.yml")
)


app = modal.App("ctt-elt-jobs", image=image)

dlt_volume = modal.Volume.from_name("ctt-dlt-state", create_if_missing=True)


@app.function(
    schedule=modal.Cron("30 22 * 10-12,1-4 3"),
    secrets=[modal.Secret.from_name("ctt-secrets")],
    volumes={"/root/.dlt": dlt_volume},
    retries=2,
    timeout=600,
)
def wednesday():
    etl_job()
    dlt_volume.commit()



@app.function(
    schedule=modal.Cron("30 2,6 * 10-12,1-4 4"),
    secrets=[modal.Secret.from_name("ctt-secrets")],
    volumes={"/root/.dlt": dlt_volume},
    retries=2,
    timeout=600,
)
def thursday():
    etl_job()
    dlt_volume.commit()


@app.function(
    schedule=modal.Cron("30 11,18,23 * 10-12,1-4 6"),
    secrets=[modal.Secret.from_name("ctt-secrets")],
    volumes={"/root/.dlt": dlt_volume},
    retries=2,
    timeout=600,
)
def saturday():
    etl_job()
    dlt_volume.commit()

