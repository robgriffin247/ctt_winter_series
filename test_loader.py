import os
import json
import httpx
import time

fetch_epoch = int(time.time() * 1000)

cookie_string = f"""{os.getenv("ZP_COOKIE")}"""

cookies = {}

for item in cookie_string.split('; '):
    if '=' in item:
        key, value = item.split('=', 1)
        cookies[key] = value


def get_series_events(series_id="CTT"):

    with httpx.Client(cookies=cookies) as client:
        series_events_response = client.get(f'https://zwiftpower.com/api3.php?do=series_event_list&id={series_id}&_={fetch_epoch}')
        series_events_response.raise_for_status()

        series_events_data = json.loads(series_events_response.content).get("data")
        for d in series_events_data:
            yield d


def get_event_results(event_id):
    
    with httpx.Client(cookies=cookies) as client:
        results_response = client.get(f'https://zwiftpower.com/cache3/results/{event_id}_zwift.json?_={fetch_epoch}')
        results_response.raise_for_status()
        results_data = json.loads(results_response.content).get("data")

        for d in results_data:
            yield d

def get_event_segment_results(event_id):

    with httpx.Client(cookies=cookies) as client:
            
        sprints_response = client.get(f'https://zwiftpower.com/api3.php?do=event_sprints&zid={event_id}')
        sprints_data = json.loads(sprints_response.content).get("data")

        for d in sprints_data:
            yield d


if __name__=="__main__":
    event_ids = []
    series_event_data = get_series_events()
    for d in series_event_data:
        if d.get("r")!="": # Return IDs for completed events (r=number of riders in results (finishers - dq'd))
            event_ids += [int(d.get("zid"))]

    for id in event_ids:
        results = get_event_results(id)
        segment_results = get_event_segment_results(id)

        for result in results:
            print(result)

        for result in segment_results:
            print(result)

        time.sleep(5)