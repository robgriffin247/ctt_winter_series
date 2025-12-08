import zpdatafetch
import keyring
import os
import json
import httpx
import time

event_id = 5188741
fetch_epoch = int(time.time() * 1000)

# Your cookies - parse them into a dict
cookie_string = f"""{os.getenv("ZP_COOKIE")}"""

# Parse into a dictionary
cookies = {}
for item in cookie_string.split('; '):
    if '=' in item:
        key, value = item.split('=', 1)
        cookies[key] = value

# Make a request with httpx
with httpx.Client(cookies=cookies) as client:
        
    series_events_response = client.get('https://zwiftpower.com/api3.php?do=series_event_list&id=CTT&_=1764341087824')
    series_events_data = json.loads(series_events_response.content).get("data")
    for d in series_events_data[0:1]:
        print([d.get("zid"), d.get("km"), d.get("tm"), d.get("f_t")])
        print("")


    results_response = client.get(f'https://zwiftpower.com/cache3/results/{event_id}_zwift.json?_={fetch_epoch}')
    results_data = json.loads(results_response.content).get("data")

    for d in results_data[0:3]:
        print(d)
        print("")

    sprints_response = client.get(f'https://zwiftpower.com/api3.php?do=event_sprints&zid={event_id}') # sprints
    sprints_data = json.loads(sprints_response.content).get("data")

    for d in sprints_data[0:3]:
        print(d)
        print("")
# keyring.set_password("zpdatafetch", "username", os.getenv("ZPUSER"))
# keyring.set_password("zpdatafetch", "password", os.getenv("ZPPASS"))

# event_id=5205108
# sprints = zpdatafetch.Result()
# sprints.fetch(event_id)
# sprints_json = json.loads(sprints.json())
# print(sprints_json)