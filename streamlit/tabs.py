import streamlit as st
import polars as pl
import plotly.express as px
import pycountry
import duckdb

def render_standings(results):
        
    def get_standings(results, selected_gender_category, selected_power_category, selected_age_category, selected_round, selected_clubs, selected_riders):
        if selected_gender_category=="Mens":
            results = results.filter(pl.col("gender")=="M")
            results = results.with_columns(pl.col("mixed_category").alias("power_category"))

        elif selected_gender_category=="Womens":
            results = results.filter(pl.col("gender")=="F")
            results = results.with_columns(pl.col("womens_category").alias("power_category"))

        else:
            results = results.with_columns(pl.col("mixed_category").alias("power_category"))

        if selected_power_category!="All":
            results = results.filter(pl.col("power_category")==selected_power_category)

        if selected_age_category!="All":
            results = results.filter(pl.col("age_category")==selected_age_category)
        
        if selected_round=="Completed":
            results = results.filter(pl.col("round_completed"))

        if selected_round not in ["All", "Completed"]:
            results = results.filter(pl.col("round_id")==selected_round)
    
        if selected_clubs!=[]:
            results = results.filter(pl.col("club").is_in(selected_clubs))
        
        with duckdb.connect() as con:

            standings = con.sql(f"""
                            with 

                            rankings_per_round as (
                                select *,
                                    rank() over (partition by is_best_effort_in_round, round_id order by race_seconds) as race_position,
                                    rank() over (partition by is_best_effort_in_round, round_id order by segment_seconds) as fts_position,
                                    sum(1) over (partition by is_best_effort_in_round, round_id) as of_riders
                            from results
                            where is_best_effort_in_round
                            order by round_id, race_position
                            ),

                            fts_bonuses as (
                            select *,
                                case when fts_position<=5 then fts_position-6 else 0 end as fts_bonus
                            from rankings_per_round    
                            ),

                            round_scores as (
                            select *, race_position + fts_bonus as round_score from fts_bonuses
                            ),

                            rank_rounds_per_rider as (
                                select *,
                                    row_number() over (partition by rider_id, route_type order by round_score) as rider_round_ranking
                                from round_scores
                            ),

                            best_seven_rounds as (
                            select * 
                            from rank_rounds_per_rider
                            where 
                                (route_type='flat' and rider_round_ranking<=4) or 
                                (route_type='rolling' and rider_round_ranking<=2) or 
                                (route_type='mountain' and rider_round_ranking<=1) 
                            ),
                            
                            pb_counts as (
                            select rider_id, -count(*) as pb_bonus from results where is_new_pb group by rider_id
                            ),

                            scores_per_rider as (
                            -- Also count races and race per type
                            select
                                best_seven_rounds.rider_id,
                                rider, club, gender, age_category, power_category, country, 
                                sum(1) as qualifying_races,
                                sum(race_position) as position_points,
                                sum(fts_bonus) as fts_bonus,
                                coalesce(last(pb_counts.pb_bonus), 0) as pb_bonus,
                                50 + sum(round_score) + coalesce(last(pb_counts.pb_bonus), 0) as score
                            from best_seven_rounds
                                left join pb_counts using(rider_id)
                            group by best_seven_rounds.rider_id, rider, club, gender, age_category, power_category, country

                            ),
                            
                            
                            filter_riders as (
                            select * from scores_per_rider
                            ),
                                
                            add_rank as (
                                select rank() over (order by qualifying_races desc, score, power_category) as rank, rider, club, power_category, gender, qualifying_races, score, position_points, fts_bonus, pb_bonus,
                                from scores_per_rider  
                            )

                            select * from add_rank
                            order by rank, position_points, fts_bonus, pb_bonus, qualifying_races, rider
                        """).pl()

            if selected_riders!=[]:
                standings = standings.filter(pl.col("rider").is_in(selected_riders))

        return standings

    st.markdown("")
    c1, c2, c3, c4 = st.columns(4)
    c5, c6 = st.columns([2,3])
    st.markdown("")
    st.markdown("")

    gender_category = c1.selectbox("Gender Category", options=["Open", "Mens", "Womens"], key="standings_gender_category")
    power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"], key="standings_power_category")
    age_category = c3.selectbox("Age Category", options=["All", "Jnr", "U23", "Snr", "Mas", "Vet", "50+", "60+", "70+", "80+"], key="standings_age_category")
    round = c4.selectbox("Round", options=["All", "Completed"]+results["round_id"].unique().sort().to_list(), key="standings_round")
    clubs = c5.multiselect("Club(s)", options=results[["club", "club_id"]].unique().sort(pl.col("club"))["club"], key="standings_clubs")
    riders = c6.multiselect("Rider(s)", options=results.filter(pl.col("club").is_in(clubs) if len(clubs)>0 else True)[["rider", "rider_id"]].unique().sort(pl.col("rider"))["rider"], key="standings_riders")

    standings = get_standings(results, gender_category, power_category, age_category, round, clubs, riders)

    if standings.shape[0]==0:
        st.write("Uh oh! No data found &mdash; try a different combination!")
    else:
        st.dataframe(standings,
                    column_config={
                        "rank": st.column_config.NumberColumn("Rank"),
                        "rider": st.column_config.TextColumn("Rider"),
                        "club": st.column_config.TextColumn("Club"),
                        "gender": st.column_config.TextColumn("Gen."),
                        "power_category": st.column_config.TextColumn("Cat."),
                        "qualifying_races": st.column_config.NumberColumn("Races"),
                        "score": st.column_config.NumberColumn("Points"),
                        "position_points": st.column_config.NumberColumn("Pos."),
                        "fts_bonus": st.column_config.NumberColumn("FTS"),
                        "pb_bonus": st.column_config.NumberColumn("PB"),
                    },
                )

        st.markdown("""
        -----
        The standings table is calculated from each riders best seven qualifying races; the points are calculated dynamically based on the gender, power and age categories, and on the selected clubs. Riders are filtered *after* the leaderboard is calculated &mdash; search for a rider and you will see their position given the combination of categories and clubs, perfect for a bit of internal club competition 😉 

        - *Rank* = rank within selected power and gender category; sorted on races completed, then points
        - *Cat* = power category A-D
        - *Gen* = gender (self-reported male or female)
        - *Races* = number of rounds completed and contributing to score (up to 7)
        - *Points* = total points = 50 + positions - fts points - pb points
        - *Pos.* = position points (sum of positions from contributing rounds)
        - *FTS* = total fts points from contributing rounds
        - *PB* = total PBs **in whole series**
        """)

def render_results(results):
    st.markdown("")
    c1, c2, c3, c4 = st.columns(4)
    c5, c6, c7, c8 = st.columns([2,3,2,2], vertical_alignment="bottom")
    st.markdown("")
    st.markdown("")

    gender_category = c1.selectbox("Gender Category", options=["Open", "Mens", "Womens"], key="results_gender_category")
    power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"], key="results_power_category")
    age_category = c3.selectbox("Age Category", options=["All", "Jnr", "U23", "Snr", "Mas", "Vet", "50+", "60+", "70+", "80+"], key="results_age_category")
    
    if gender_category=="Mens":
        results = results.filter(pl.col("gender")=="M")
        results = results.with_columns(pl.col("mixed_category").alias("power_category"))

    elif gender_category=="Womens":
        results = results.filter(pl.col("gender")=="F")
        results = results.with_columns(pl.col("womens_category").alias("power_category"))

    else:
        results = results.with_columns(pl.col("mixed_category").alias("power_category"))

    if power_category!="All":
        results = results.filter(pl.col("power_category")==power_category)

    if age_category!="All":
        results = results.filter(pl.col("age_category")==age_category)
    
    clubs = c5.multiselect("Club(s)", options=results[["club", "club_id"]].unique().sort(pl.col("club"))["club"], key="results_clubs")
    if clubs!=[]:
        results = results.filter(pl.col("club").is_in(clubs))

    riders = c6.multiselect("Rider(s)", options=results[["rider", "rider_id"]].unique().sort(pl.col("rider"))["rider"], key="results_riders")
    if riders!=[]:
        results = results.filter(pl.col("rider").is_in(riders))

    routes = c7.multiselect("Route(s)", options=results[["route"]].unique().sort(pl.col("route"))["route"], key="results_route")
    if routes!=[]:
        results = results.filter(pl.col("route").is_in(routes))

    rounds = c4.multiselect("Round(s)", options=results[["round_id"]].unique().sort(pl.col("round_id"))["round_id"], key="results_round")
    if rounds!=[]:
        results = results.filter(pl.col("round_id").is_in(rounds))

    
    if c8.toggle("Round Bests Only", value=True):
        results = results.filter(pl.col("is_best_effort_in_round")==True)

    if results.shape[0]==0:
        st.write("Uh oh! No data found &mdash; try a different combination!")
    else:
        st.dataframe(results[["round_id", "rider", "club", "country", "gender", "age_category", "categories", "watts_average", "wkg_average", "race_time", "race_speed", "is_best_effort_in_round", "is_new_pb", "segment_time", "start_datetime_utc", "route"]].sort([pl.col("round_id"), pl.col("race_speed")], descending=[False, True]),
                    column_config={
                        "round_id":st.column_config.NumberColumn("Round", pinned=True),
                        "rider":st.column_config.TextColumn("Rider", pinned=True),
                        "club":st.column_config.TextColumn("Club"),
                        "country":st.column_config.TextColumn("Country"),
                        "gender":st.column_config.TextColumn("Gender"),
                        "age_category":st.column_config.TextColumn("Age"),
                        "categories":st.column_config.TextColumn("Category"),
                        #"mixed_category":st.column_config.TextColumn("Cat."),
                        #"womens_category":st.column_config.TextColumn("(W)"),
                        "watts_average":st.column_config.NumberColumn("Watts", format="%.0f"),
                        "wkg_average":st.column_config.NumberColumn("W/kg", format="%0.1f"),
                        "race_time":st.column_config.TextColumn("Time"),
                        #"race_seconds":st.column_config.TextColumn("Time"),
                        "race_speed":st.column_config.NumberColumn("Speed (km/h)", format="%.2f"),
                        "is_best_effort_in_round":st.column_config.CheckboxColumn("RB"),
                        "is_new_pb":st.column_config.CheckboxColumn("PB"),
                        "segment_time":st.column_config.TextColumn("Segment"),
                        #"segment_seconds":st.column_config.TextColumn("Segment"),
                        "start_datetime_utc":st.column_config.DatetimeColumn("Date/Time", format="localized"),
                        "route":st.column_config.TextColumn("Route"),
                        "route_type":st.column_config.TextColumn("Type"),
                    })

        if len(riders)==1:
            c1, c2, c3 = st.columns(3)
            c1.metric("Flat", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='flat').shape[0]} of 4", border=True)
            c2.metric("Rolling", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='rolling').shape[0]} of 2", border=True)
            c3.metric("Mountain", f"{results.filter(pl.col('is_best_effort_in_round')).filter(pl.col('route_type')=='mountain').shape[0]} of 1", border=True)

        st.markdown("""
                    -----
                    This table shows every effort from across the series; you can find your round-bests (RB; the fastest attempts per round, those that determine your score for the round).
                    """)

def render_stats(results, christmas=False):

    def power_figure(data):

        c1, c2 = st.columns([6,3])
        c2.markdown("")
        c2.markdown("")
        x_metric = c2.selectbox("X-Axis Value", options=["W/kg", "Watts",])
        y_metric = c2.selectbox("Y-Axis Value", options=["Race Speed", "Race Time", "Segment Speed", "Segment Time"])
        c_metric = c2.selectbox("Colour by", options=["Category", "Route" if "Race" in y_metric else "Segment", "Gender",])
        
        if "Race" in y_metric:
            selected_routes = c2.multiselect("Route(s)", options=data[["route"]].unique().sort(pl.col("route"))["route"].to_list())
            selected_segments = []
            
        if "Segment" in y_metric:
            selected_routes = []
            selected_segments = c2.multiselect("Segment(s)", options=data[["segment"]].unique().sort(pl.col("segment"))["segment"].to_list())
            
        selected_power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"])
        
        if c_metric=="Category":
            colour_by = "mixed_category"      
        if c_metric=="Gender":
            colour_by = "gender"        
        if c_metric=="Route":
            colour_by = "route"      
        if c_metric=="Segment":
            colour_by = "segment"

        if "Race" in y_metric:
            if x_metric=="W/kg":
                x_column = "wkg_average"
            if x_metric=="Watts":
                x_column = "watts_average"
        if "Segment" in y_metric:
            if x_metric=="W/kg":
                x_column = "segment_wkg"
            if x_metric=="Watts":
                x_column = "segment_watts"
        
        if y_metric=="Race Speed":
            y_column = "race_speed"        
        if y_metric=="Race Time":
            y_column = "race_seconds"
        if y_metric=="Segment Speed":
            y_column = "segment_speed"        
        if y_metric=="Segment Time":
            y_column = "segment_seconds"

        fig = px.scatter(
            data
                .filter(pl.col("route").is_in(selected_routes) if len(selected_routes)>0 else True)
                .filter(pl.col("segment").is_in(selected_segments) if len(selected_segments)>0 else True)
                .filter(pl.col("mixed_category")==selected_power_category if selected_power_category!="All" else True)
                .sort(pl.col("mixed_category")),
            x=x_column,
            y=y_column,
            color=colour_by,
            color_discrete_sequence=[
                "#fc4219",
                "#59c34e",
                "#3fc1e9",
                "#fbcf0c",
                "#9b59b6",
                "#e74c3c",
                "#1abc9c",
                "#34495e" 
            ],
            opacity=0.4,
            height=600,
            labels={
                "watts_average": "Average Power (W)",
                "segment_watts": "Average Power (W)",
                "wkg_average": "Average Power (W/kg)",
                "segment_wkg": "Average Power (W/kg)",
                "race_seconds": "Time (seconds)",
                "race_speed": "Speed (km/h)",
                "segment_speed": "Speed (km/h)",
                "segment_seconds": "Time (seconds)",
                "mixed_category": "Category",
                "gender": "Gender",
                "route": "Route",
                "segment": "Segment",
            },    hover_data={"rider": True, "mixed_category": True},
        )

        # Customize hover template
        x_label = "Average Power (W)" if x_metric == "Watts" else "Average Power (W/kg)"
        y_label = "Time (seconds)" if y_metric == "Time" else "Speed (km/h)"

        fig.update_traces(
            hovertemplate='<b>%{customdata[0]} (%{customdata[1]})</b><br>' +
                        f'{x_label}=%{{x}}<br>' +
                        f'{y_label}=%{{y}}' +
                        '<extra></extra>'
        )
        
        c1.plotly_chart(fig)

    def world_map(results):
        # Create ISO-2 to ISO-3 mapping
        iso2_to_iso3 = {country.alpha_2: country.alpha_3 for country in pycountry.countries}
        
        # Create ISO-2 to country name mapping
        iso2_to_name = {country.alpha_2: country.name for country in pycountry.countries}
        
        # Aggregate: count total rows and unique riders per country
        country_counts = (
            results
            .group_by('country')
            .agg([
                pl.len().alias('efforts'),
                pl.col('rider_id').n_unique().alias('riders')
            ])
            .with_columns([
                pl.col('country').replace(iso2_to_iso3).alias('iso3'),
                pl.col('country').replace(iso2_to_name).alias('country_name')
            ])
        )

        # Create the choropleth map
        fig = px.choropleth(
            country_counts,
            locations='iso3',
            locationmode='ISO-3',
            color='efforts',
            color_continuous_scale=[[0, 'white'], [0.001, "#ffeb9a"], [0.2, '#fc6719'], [1, '#fc6719']],
            range_color=[0, country_counts['efforts'].max()],
            custom_data=['country_name', 'efforts', 'riders'],
        )

        
        # Update hover template with your exact format
        fig.update_traces(
            hovertemplate='<b>%{customdata[0]}</b><br>' +
                        'Riders: %{customdata[2]}<br>' +
                        'Efforts: %{customdata[1]}' +
                        '<extra></extra>'
        )

        fig.update_layout(
            coloraxis_showscale=False,  # This removes the color legend
            title=None,
            geo=dict(
                showframe=False,
                showcoastlines=True,
                coastlinecolor='#e0e0e0',  # Light gray coastlines
                countrycolor='#d0d0d0',    # Light gray country borders
                projection_type='natural earth',
                bgcolor='rgba(0,0,0,0)',
                lataxis_range=[-60, 90]  # This cuts off Antarctica
            ),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=0, b=0),
        )

        st.plotly_chart(fig, width='stretch')
        
    distance = sum(results["route_length"])
    hours = sum(results["race_seconds"])/3600
    kwhs = sum((results["watts_average"]/1000) * (results["race_seconds"]/3600))

    st.markdown("")
    c1, c2, c3 = st.columns(3)
    c4, c5, c6 = st.columns(3)
    c7, c8, c9 = st.columns(3)

    if not christmas or st.toggle("Bah! I don't like christmas!", value=False):
        c1.metric("Riders 🚴‍♂️", f"{results[["rider_id"]].unique().shape[0]:,.0f}", border=True)
        c2.metric("Efforts 🏁", f"{results.shape[0]:,.0f}", border=True)
        c3.metric("PBs 🏆", f"{sum(results["is_new_pb"]):,.0f}", border=True)
        c4.metric("Distance 🌍", f"{distance:,.0f} km", border=True)
        c5.metric("Hours ⏱️", f"{hours:,.0f}", border=True)
        c6.metric("Everests Climbed 🏔️", f"{sum(results["route_elevation"])/8848:,.2f}", border=True)
        c7.metric("Energy Generated ⚡", f"{kwhs:,.0f} kW/h", border=True)
        c8.metric("Calories Burned 🔥", f"{kwhs * 860.420 / 0.24:,.0f}", border=True)
        c9.metric("Pizza Slices 🍕", f"{kwhs * 860.420 / 0.24 / 266:,.0f}", border=True)
    else:
        c1.metric("Santas 🎅", f"{results[["rider_id"]].unique().shape[0]:,.0f}", border=True, help="Riders")
        c2.metric("Sleds Raced 🛷", f"{results.shape[0]:,.0f}", border=True, help="Efforts")
        c3.metric("Gifts Received 🎁", f"{sum(results["is_new_pb"]):,.0f}", border=True, help="PBs beaten")
        c4.metric("Sleigh Rides Around the World 🌍", f"{distance/40075:,.2f}", border=True, help=f"{distance:,.0f} km")
        #c5.metric("Christmas Days ⏱️", f"{hours/24:,.2f}", border=True, help="Cumulative days of riding")
        c5.metric("Die Hard Showings 🎬", f"{hours*60/132:,.1f}", border=True, help=f"{hours:,.0f} cumulative hours of riding; Christmas cracker Die Hard is 2h 12m")
        c6.metric("Mt Gunnbjørns Climbed 🏔️", f"{sum(results["route_elevation"])/3694:,.1f}", border=True, help=f"{sum(results["route_elevation"]):,.0f} meters climbed; Mt Gunnbjørn his the highest peak in the Arctic Circle at 3694 metres above sea level")
        c7.metric("Christmas Lights Lit 🎄", f"{kwhs/(5.65/100):,.0f} m", border=True, help=f"{kwhs:.0f} kW/hs; based on 7.6 W per 100 metres for all of December") # 5.65 per 100m; 5.65/100 = per metre
        c8.metric("Logs Burned 🔥", f"{kwhs * 860.420 / 0.24 / 1900:,.1f} kg", border=True, help=f"{kwhs * 860.420 / 0.24:,.0f} calories") # 1900 cal/kg
        c9.metric("Turkey Legs Eaten 🍗", f"{kwhs * 860.420 / 0.24 / 416:,.0f}", border=True, help=f"{kwhs * 860.420 / 0.24 / 266:,.0f} pizza slices")

    
    power_figure(results)
    world_map(results)

def render_schedule(rounds, winners):
    st.markdown("")
    st.markdown("""
                Riders need to complete four flat rounds, two rolling rounds and one mountain round over 14 weeks to compete for the 2025/26 crown. 
                Sign up for our upcoming races at [Zwift](https://zwiftinc.sjv.io/c/2607378/1772639/20902?u=https%3A%2F%2Fwww.zwift.com%2Fevents%2Ftag%2Fcyclingtimetrials)!""")
    st.markdown("")

    for r in rounds.iter_rows(named=True):
        with st.expander(f"{r['round_id']}: {r['route']}, {r['date_range']}"):
            c1, c2 = st.columns(2)
            with c1:
                st.html(f"""<p style="margin-bottom:0">
                <strong>Route:</strong> <a href="{r['route_link']}" target="_blank">{r['route']}</a> ({r['route_type'][0].upper()+r['route_type'][1:]})
                </p><p style="margin-bottom:0">
                <strong>Length:</strong> {r['route_length']:.1f} km / {(float(r['route_length'])*0.621):.1f} mi
                </p>
                <strong>Climb:</strong> {r['route_elevation']:.1f} m / {(float(r['route_length'])*3.281):.1f} ft ({r['route_gradient']:.1f}%)
                """)
            with c2:
                st.html(f"""<p style="margin-bottom:0">
                <strong>Route:</strong> <a href="{r['segment_link']}" target="_blank">{r['segment']}</a>
                </p><p style="margin-bottom:0">
                <strong>Length:</strong> {r['segment_length']:.1f} km / {(float(r['segment_length'])*0.621):.1f} mi
                </p>
                <strong>Climb:</strong> {r['segment_elevation']:.1f} m / {(float(r['segment_length'])*3.281):.1f} ft ({r['segment_gradient']:.1f}%)
                """)

            
            if r["mens"] is not None:
                st.dataframe(winners.filter(pl.col("round_id")==r["round_id"])[["rider", "category", "gender"]].pivot(
                    index='category',
                    on='gender',
                    values='rider').sort('category'),
                    column_config={
                        "category":st.column_config.TextColumn("Category", width=90),
                        "F":st.column_config.TextColumn("🏆 Womens", width=280),
                        "M":st.column_config.TextColumn("🏆 Mens", width=280),
                    })


