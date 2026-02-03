import zpdatafetch
import keyring
import os
import json
import dlt
import sys

from typing import Any
from collections.abc import Iterator
from dlt.extract import DltResource
from dlt.common.pipeline import LoadInfo

keyring.set_password("zpdatafetch", "username", os.getenv("ZPUSER"))
keyring.set_password("zpdatafetch", "password", os.getenv("ZPPASS"))

target = os.getenv("TARGET")


@dlt.resource(
    name="sprint_results",
    write_disposition="merge",
    primary_key=(
        "zid",
        "zwid",
    ),
)
def get_sprints(event_id) -> Iterator[dict[str, Any]]:

    print(f"Getting sprints for event {event_id}")

    sprints = zpdatafetch.Sprints()
    sprints.fetch(event_id)
    sprints_json = json.loads(sprints.json())
    riders = sprints_json.get(
        f"{event_id}" if isinstance(event_id, int) else event_id
    ).get("data")
    for rider in riders:
        yield rider


@dlt.source
def zpdatafetch_source(payload) -> list[DltResource[Any]]:
    return [get_sprints(payload)]


def ingest_zpdatafetch(payload) -> LoadInfo:
    if target in ["prod", "dev"]:
        _destination = dlt.destinations.motherduck(
            credentials={
                "database": f"ctt_winter_series_{target}",
                "motherduck_token": os.environ["MOTHERDUCK_TOKEN"],
            }
        )
    elif target == "test":
        _destination = dlt.destinations.duckdb(
            credentials=f"data/ctt_winter_series_{target}.duckdb"
        )
    else:
        raise ValueError(
            f"Invalid TARGET value: {target} (should be one of dev, test and prod - ensure env variables have been exported)."
        )
    pipeline = dlt.pipeline(
        pipeline_name=f"ctt_winter_series_{target}_pipeline",
        destination=_destination,
        dataset_name="zpdatafetch",
    )

    """
    Run the pipeline!
    """
    load_info = pipeline.run(zpdatafetch_source(payload))

    return load_info


if __name__ == "__main__":
    event_id = sys.argv[1] if len(sys.argv) > 1 else 5200083  # 5205102
    load_info = ingest_zpdatafetch(event_id)
    print(load_info)
