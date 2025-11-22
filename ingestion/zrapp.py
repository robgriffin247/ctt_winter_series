import os
import httpx
import dlt
import duckdb
import sys
import json
from typing import Any
from collections.abc import Iterator
from dlt.common.pipeline import LoadInfo
from dlt.extract import DltResource
import html


def ingest_zrapp(event_id) -> LoadInfo:

    header = {"Authorization": os.getenv("ZRAPP_API_KEY")}
    url = f"https://zwift-ranking.herokuapp.com/public/"  # /zp/{id}/results

    @dlt.resource(
        name="results", write_disposition="merge", primary_key="unique_event_rider_id"
    )
    def get_results(event_id: int) -> Iterator[dict[str, Any]]:
        if not isinstance(event_id, int):
            raise TypeError(f"ID must be an integer, got {event_id!r}")

        print(f"Getting results for event {event_id}")

        response = httpx.get(f"{url}zp/{event_id}/results", headers=header)
        response.raise_for_status()

        riders = response.json()

        INT_VALUES = {
            "zid": "event_id",
            "zwid": "rider_id",
            "tid": "club_id",
            "male": "gender_numeric",
            "pos": "zp_position",
            "position_in_cat": "category_position",
        }

        FLT_VALUES = {
            "gap": "gap_seconds",
            "time_gun": "time_seconds",
        }

        STR_VALUES = {
            "name": "rider",
            "category": "category",
            "tname": "club",
        }

        for rider in riders:
            out = {}

            for key, value in INT_VALUES.items():
                raw = rider.get(key)
                if isinstance(raw, str) and raw.strip() == "":
                    raw = None
                out[value] = int(raw) if raw is not None else None

            for key, value in FLT_VALUES.items():
                raw = rider.get(key)
                if isinstance(raw, str) and raw.strip() == "":
                    raw = None
                out[value] = float(raw) if raw is not None else None

            for key, value in STR_VALUES.items():
                raw = rider.get(key)
                if raw is not None:
                    out[value] = html.unescape(str(raw))
                else:
                    out[value] = None

            LISTED_VALUES = {
                "weight": "weight",
                "np": "watts_normalised",
                "avg_power": "watts_average",
                "w1200": "watts_1200s",
                "w300": "watts_300s",
                "w120": "watts_120s",
                "w60": "watts_60s",
                "w30": "watts_30s",
                "w15": "watts_15s",
                "w5": "watts_5s",
            }

            for key, value in LISTED_VALUES.items():
                raw = rider.get(key)[0]
                if isinstance(raw, str) and raw.strip() == "":
                    raw = None
                out[value] = float(raw) if raw is not None else None

            out["unique_event_rider_id"] = (
                f'{out.get("event_id")}_{out.get("rider_id")}'
            )

            yield out

    @dlt.source
    def zrapp_source(event_id) -> list[DltResource[Any]]:
        return [get_results(event_id)]

    target = os.getenv("TARGET")

    if target == "prod":
        _destination = dlt.destinations.motherduck(
            credentials={
                "database": "ctt_winter_series_prod",
                "motherduck_token": os.environ["MOTHERDUCK_TOKEN"],
            }
        )
    if target == "dev":
        _destination = dlt.destinations.motherduck(
            credentials={
                "database": "ctt_winter_series_dev",
                "motherduck_token": os.environ["MOTHERDUCK_TOKEN"],
            }
        )
    if target == "test":
        _destination = dlt.destinations.duckdb(
            credentials="data/ctt_winter_series_test.duckdb"
        )

    pipeline = dlt.pipeline(
        pipeline_name=f"ctt_winter_series_{target}_pipeline",
        destination=_destination,
        dataset_name="zrapp",
    )

    load_info = pipeline.run(zrapp_source(event_id))

    return load_info


if __name__ == "__main__":
    print(ingest_zrapp(int(sys.argv[1])))
