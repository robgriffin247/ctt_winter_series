import httpx
import os
import time
import duckdb
import polars as pl
import json

def get_rider(id):
    header = {"Authorization": os.getenv("ZRAPP_API_KEY")}
    url = f"https://zwift-ranking.herokuapp.com/public/riders/{id}" 
    response = httpx.get(url, headers=header)
    response.raise_for_status()

    rider = response.json()
    return rider

def get_riders(ids):
    header = {"Authorization": os.getenv("ZRAPP_API_KEY")}
    url = f"https://zwift-ranking.herokuapp.com/public/riders/" 
    response = httpx.post(url, headers=header, json=ids)
    response.raise_for_status()
    riders = response.json()
    return riders

def get_zp_result(id):
    header = {"Authorization": os.getenv("ZRAPP_API_KEY")}
    url = f"https://zwift-ranking.herokuapp.com/public/zp/{id}/results/" 
    response = httpx.get(url, headers=header)
    response.raise_for_status()

    results = response.json()
    return results

def get_result(id):
    header = {"Authorization": os.getenv("ZRAPP_API_KEY")}
    url = f"https://zwift-ranking.herokuapp.com/public/results/{id}/" 
    response = httpx.get(url, headers=header)
    response.raise_for_status()

    results = response.json()
    return results

if __name__=="__main__":
    riders = get_zp_result(5145077)
    for rider in riders:
        if rider.get("zwid")==651801:
            print(rider)
    # with duckdb.connect("data/ctt_winter_series_test.duckdb") as con:
    #     ids = con.sql("select rider_id from staging.stg_results where weight=0").pl()["rider_id"].to_list()
    
    # riders = get_riders(ids)

    # with open("temp.json", "w") as f:
    #     f.dumps(riders)