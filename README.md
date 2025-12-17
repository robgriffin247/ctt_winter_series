# Cycling Time Trials, Winter Series 2025/26 Results Platform

#### About

Cycling Time Trials (CTT) is running a winter time trial racing series on Zwift (an online bicycle training and racing platform). Over 14 rounds, riders need to complete four flat rounds, two rolling terrain rounds and one mountain round. Riders can race multiple times per round/week (~4 events/timeslots per week). The effort with the fastest time from each round time is taken as their official effort for the round. 

All best efforts over the round are then ranked and points awarded accordingly &mdash; 1 for fastest rider, 2 for second fastest and so on. Further bonus point deductions are awarded for:

- the top five segment times from those best efforts
    - each route has a segment used for a fastest through the segment (FTS) competiton, to tempt riders to push a little harder and risk blowing up
- every effort (not just best efforts) that beats a previous PB for a course
    - a course might appear in two rounds, four timeslots per round, so 7 opportunties to set a PB on that course
    
The rider with the lowest score wins.

Raw data from two APIs is loaded to DuckDB/Motherduck, transformed with dbt and visualised using Streamlit in a [results app](https://ctt-winter-series.fly.dev) hosted on fly.io. 


Modelling involves:

- establishing a per rider max power output for categorisation
- identifying best efforts
- ranking best efforts and segment efforts within rounds
- tracking PBs over multiple rounds

The points and leaderboard are then derived on the front-end, rather than in dbt, to allow users to filter on power category, gender, age and club to generate leaderboards dynamically &mdash; while we are providing category and gender specific competitions, we also want to facilitate a bit of friendly informal competition within clubs! 

This platform is developed using a local test environment, as well as cloud dev and prod environments. I run a bash script which handles the ingestion and transformation of the data manually &mdash; this is done manually because we need organisers to perform some manual work on disqualifications and validations before we ingest the data.

#### My Role

I was approached just hours before this series kicked off as the organisers had received far more sign ups than expected, so their plan of an excel sheet and a few late nights we no longer going to cut it. There has been a focus on developing fast to get a working results platform together, and then iterate to improve functionality and develop new features.


#### Ingest, Develop & Deploy


Set the ``TARGET`` using ``export TARGET="<target>"``.

- ``test`` = local duckdb
- ``dev`` = development motherduck database, allowing testing of cloud based aspects and serving as a backup to prod
- ``prod`` = production motherduck database, where the published apps look for data

Check the values in [events](seeds/events.csv) and [rounds](seeds/rounds.csv) dbt seeds are correct &mdash; the bash script will check these to find which events to load, and events will need continually adding as they are created, while rounds has a completed column which is used in some filters.

Use the ``runner.sh`` script to ingest data; it handles dlt and dbt for the given events. It checks the event seed in dbt to identify which races are in the past and which rounds they are from. 

- Running ``uv run bash runner.sh`` will iggest all past events
- Running ``uv run bash runner.sh 123456789`` will ingest event with ID ``123456789``
- Running ``uv run bash runner.sh -r 1`` will ingest all events from round 1

Deploy the app using

- ``uv run fly deploy`` for the results users app

Adjust configs for the app in ``fly.toml``.


#### ToDo

- [ ] Add date of loading to results
    - I think DQd riders may remain as it stands (merge write-disposition)
    - Want only those in the most recent load per event
- [ ] Check what will happen when seven races complete
    - Does leaderboard limit to seven?