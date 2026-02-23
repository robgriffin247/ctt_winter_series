from jobs import etl_job

import modal
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

IMAGE = (
    modal.Image.debian_slim()
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
    .add_local_dir(PROJECT_ROOT / "macros", "/root/macros")
    .add_local_dir(PROJECT_ROOT / "seeds", "/root/seeds")
    # dbt files
    .add_local_file(PROJECT_ROOT / "dbt_project.yml", "/root/dbt_project.yml")
    .add_local_file(PROJECT_ROOT / "profiles.yml", "/root/profiles.yml")
)

dlt_volume = modal.Volume.from_name("ctt-dlt-state", create_if_missing=True)

SECRETS = [modal.Secret.from_name("ctt-secrets")]
VOLUMES = {"/root/.dlt": dlt_volume}
TIMEOUT = 600
RETRIES = 2

app = modal.App("ctt-elt-jobs", image=IMAGE)

@app.function(
    schedule=modal.Cron("30 22 * 10-12,1-4 3"),
    secrets=SECRETS,
    volumes=VOLUMES,
    timeout=TIMEOUT,
    retries=RETRIES,
)
def wednesday():
    print(etl_job())
    dlt_volume.commit()
    print("Volume commited")



@app.function(
    schedule=modal.Cron("30 2,6,18 * 10-12,1-4 4"),
    secrets=SECRETS,
    volumes=VOLUMES,
    timeout=TIMEOUT,
    retries=RETRIES,
)
def thursday():
    etl_job()
    dlt_volume.commit()
    print("Volume commited")


@app.function(
    schedule=modal.Cron("30 11,18,23 * 10-12,1-4 6"),
    secrets=SECRETS,
    volumes=VOLUMES,
    timeout=TIMEOUT,
    retries=RETRIES,
)
def saturday():
    etl_job()
    dlt_volume.commit()
    print("Volume commited")


@app.function(
    schedule=modal.Cron("40 3,7 * 10-12,1-4 7"),
    secrets=SECRETS,
    volumes=VOLUMES,
    timeout=TIMEOUT,
    retries=RETRIES,
)
def sunday():
    etl_job()
    dlt_volume.commit()
    print("Volume commited")
