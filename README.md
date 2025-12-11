# Cycling Time Trials, Winter Series 2025/26 Results Platform

#### About

I was approached just 24 hours before the series kicke doff as they had received far more sign ups than expected, so an excel sheet and a few late nights we no longer going to cut it &mdash; data engineer, to the rescue!

CTT ia running a winter time trial racing series on Zwift (an online bicycle training and racing platform). Over 14 rounds riders need to complete times in four flat rounds, two rolling terrain rounds and one mountain round. Riders can race multiple times per week (~4 events/timeslots per week). Each round, their best race time is taken as their best effort. Best efforts over the round are ranked and points awarded accordingly. Further bonus point deductions are awarded for the top five segment times from those best efforts, and every effort (not just best efforts) that beats a previous PB for a course. The rider with the lowest score wins. We also wanted to divide riders on power category, gender and age category.

To make this a reality, I have used a couple of APIs to get data from Zwift, loaded it to a DuckDB/MotherDuck database using dlt, transformed using dbt, and visualised [results](https://ctt-winter-series.fly.dev) (for competitors) and series analytics (for organisers) in streamlit apps hosted on fly.io and modal respectively. This is developed using a local test environment, as well as cloud dev and prod environments. I run a bash script after the organisers have checked and handled all disqualifications in the raw data, which loads the required data, transforms and puts it into production.

#### Ingest, Develop & Deploy


Set the ``TARGET`` using ``export TARGET="<target>"``.

- ``test`` = local duckdb
- ``dev`` = development motherduck database, allowing testing of cloud based aspects and serving as a backup to prod
- ``prod`` = production motherduck database, where the published apps look for data

Use the ``runner.sh`` script to ingest data; it handles dlt and dbt for the given events. It checks the event seed in dbt to identify which races are in the past and which rounds they are from. 

- Running ``uv run bash runner.sh`` will iggest all past events
- Running ``uv run bash runner.sh 123456789`` will ingest event with ID ``123456789``
- Running ``uv run bash runner.sh -r 1`` will ingest all events from round 1

Deploy the apps using

- ``uv run fly deploy`` for the results users app
- ``uv run modal deploy modal/web_admin_app.py`` for the organisers analytics app

Adjust configs for the fly app in ``fly.toml`` and modal app in ``modal/web_admin_app.py``.