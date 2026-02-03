Modal is used to automate the ETL pipeline. The job is defined in [``jobs.py``](modal/jobs.py) which:

- Reads in the round and race data from google sheets
- Collects data from ZwiftRacing and ZwiftPower for all events in the last 72 hours or any that are in the races google sheet and not in results
- Transforms the data

Schedules for running this job are created in [``schedules.py``](modal/schedules.py) which contains day-specific schedules (modal is limited to 5 cron jobs so setting times appropriate for each day works nicely). The [``transformer.py``](modal/transformer.py) is a helper function used to trigger a dbt run in modal.

Further jobs can be run on demand by going to [modal](https://modal.com/apps/cyclingtt-data/main/ap-R81vJnfmlOIyn0kXYYGU5M?activeTab=overview) and pressing run now on one of the app functions.

Deploy to modal with

    ```
    uv run modal deploy modal/schedules.py
    ```