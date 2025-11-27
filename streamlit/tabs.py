import streamlit as st
import polars as pl
import plotly.express as px

def render_standings_tab(tab, data):
    with tab:

        st.write("  ")
        c1, c2 = st.columns([2,5])
        st.write("  ")
        st.write("  ")

        category = c1.selectbox(
            "Category", options=["All", "A", "B", "C", "D"], key=f"category_standings"
        )
        data = data.filter(pl.col("power_category") == category if category!="All" else True)

        gender = c1.selectbox("Gender", options=["Mixed", "Mens", "Womens"], key=f"gender_standings")
        data = data.filter(pl.col("gender_category") == gender)

        clubs = c2.multiselect(
            "Club(s)",
            options=data.sort(pl.col("club"))[["club"]].unique(),
            key=f"clubs_standings",
        )

        data = (
            data.filter(pl.col("club").is_in(clubs))
            if len(clubs) > 0
            else data
        )

        riders = c2.multiselect(
            "Rider(s)",
            options=data.sort(pl.col("rider"))[["rider"]].unique(),
            key=f"riders_standings",
        )

        data = (
            data.filter(pl.col("rider").is_in(riders))
            if len(riders) > 0
            else data
        )

        columns = ["rank","rider","race_count","score","position_points","segment_bonuses","pb_bonuses",]
        if category=="All":
            columns = ["rank","rider","power_category","race_count","score","position_points","segment_bonuses","pb_bonuses",]

        if data.shape[0]==0:
            st.write("No riders found - check the power and gender category is correct!")


        else:
            st.dataframe(
                data[columns],
                column_config={
                    "rank": st.column_config.NumberColumn("#", width=16),
                    "rider": st.column_config.TextColumn("Rider"),
                    "power_category": st.column_config.TextColumn("Cat."),
                    "race_count": st.column_config.NumberColumn("Races"),
                    "score": st.column_config.NumberColumn("Points"),
                    "position_points": st.column_config.NumberColumn("Pos."),
                    "segment_bonuses": st.column_config.NumberColumn("FTS"),
                    "pb_bonuses": st.column_config.NumberColumn("PB"),
                },
            )

            st.markdown("""
            -----
            The leaderboard is calculated from your best seven qualifying efforts (see *Best Efforts*). 

            - *#* = rank within selected power and gender category; sorted on races completed, then points
            - *Races* = number of rounds completed and contributing to score (up to 7)
            - *Points* = total points = 50 + positions - fts points - pb points
            - *Pos.* = position points (sum of positions from contributing rounds)
            - *FTS* = total fts points from contributing rounds
            - *PB* = total PBs **in whole series**
            """)

def render_best_efforts_tab(tab, data):
    with tab:
        
        st.write("  ")
        c1, c2, c3 = st.columns([2, 2, 5])
        st.write("  ")
        st.write("  ")

        data = data[["round_id", "route", "route_type", "rider", "gender_category", "power_category", "watts_average", "wkg_average", "race_rank", "race_time", "race_speed", "segment_rank", "segment_time"]]
        
        category = c1.selectbox(
            "Category", options=["All", "A", "B", "C", "D"], key=f"category_bests"
        )
        data = data.filter(pl.col("power_category") == category if category!="All" else True)

        gender = c2.selectbox("Gender", options=["Mixed", "Mens", "Womens"], key=f"gender_bests")
        data = data.filter(pl.col("gender_category") == gender)

        riders = c3.multiselect(
            "Rider(s)",
            options=data.sort(pl.col("rider"))[["rider"]].unique(),
            key=f"riders_bests",
        )

        data = (
            data.filter(pl.col("rider").is_in(riders))
            if len(riders) > 0
            else data
        )

        st.dataframe(data.sort([pl.col("round_id"), pl.col("power_category"), pl.col("race_speed")], descending=[False, False, True]),
                    column_config={
                        "round_id":st.column_config.NumberColumn("Rnd"),
                        "route":st.column_config.TextColumn("Route"),
                        "route_type":st.column_config.TextColumn("Type"),
                        "rider":st.column_config.TextColumn("Rider"),
                        "gender_category":st.column_config.TextColumn("Gen."),
                        "power_category":st.column_config.TextColumn("Cat."),
                        "watts_average":st.column_config.NumberColumn("Watts", format="%.0f"),
                        "wkg_average":st.column_config.NumberColumn("W/kg", format="%0.1f"),
                        "race_rank":st.column_config.NumberColumn("Rank", format="%.0f"),
                        "race_time":st.column_config.TextColumn("Time"),
                        "race_speed":st.column_config.NumberColumn("km/h", format="%.2f"),
                        "segment_rank":st.column_config.NumberColumn("FTS Rank", format="%.0f"),
                        "segment_time":st.column_config.TextColumn("Segment"),
                    })
        
        if len(riders)==1:
            c1, c2, c3 = st.columns(3)
            c1.metric("Flat Efforts", f"{data.filter(pl.col("route_type")=="flat").shape[0]} of 4", border=True)
            c2.metric("Rolling Efforts", f"{data.filter(pl.col("route_type")=="rolling").shape[0]} of 2", border=True)
            c3.metric("Mountain Efforts", f"{data.filter(pl.col("route_type")=="mountainous").shape[0]} of 1", border=True)

        st.markdown("""
                    -----
                    This data shows the efforts contributing to the leaderboard; each riders score is determined by their four fastest efforts in a flat race, two fastest in a rolling race, and fastest in a mountainous race. Segment times from these efforts is used for FTS ranking.
                    
                    Search for a single rider and you'll see a breakdown of what race types they have completed!
                    """)

def render_race_efforts_tab(tab, data):
    with tab:
        
        st.write("  ")
        c1, c2, c3 = st.columns([2, 2, 5])
        st.write("  ")
        st.write("  ")

        data = data.filter(pl.col("gender_category")=="Mixed")[["round_id", "route", "start_datetime_utc", "rider", "gender", "power_category", "watts_average", "wkg_average", "race_time", "race_speed", "new_pb", "segment_time"]]
        
        category = c1.selectbox(
            "Category", options=["All", "A", "B", "C", "D"], key=f"category_results"
        )
        data = data.filter(pl.col("power_category") == category if category!="All" else True)

        gender = c2.selectbox("Gender", options=["Mixed", "Men", "Women"], key=f"gender_results")
        gender_char = "F" if gender=="Women" else "M"
        data = data.filter(pl.col("gender") == gender_char if gender!="Mixed" else True)

        riders = c3.multiselect(
            "Rider(s)",
            options=data.sort(pl.col("rider"))[["rider"]].unique(),
            key=f"riders_results",
        )

        data = (
            data.filter(pl.col("rider").is_in(riders))
            if len(riders) > 0
            else data
        )

        st.dataframe(data.sort([pl.col("round_id"), pl.col("power_category"), pl.col("race_speed")], descending=[False, False, True]),
                    column_config={
                        "round_id":st.column_config.NumberColumn("Rnd"),
                        "route":st.column_config.TextColumn("Route"),
                        "start_datetime_utc":st.column_config.DatetimeColumn("Date/Time", format="localized"),
                        "rider":st.column_config.TextColumn("Rider"),
                        "gender":st.column_config.TextColumn("Gen."),
                        "power_category":st.column_config.TextColumn("Cat."),
                        "watts_average":st.column_config.NumberColumn("Watts", format="%.0f"),
                        "wkg_average":st.column_config.NumberColumn("W/kg", format="%0.1f"),
                        "race_time":st.column_config.TextColumn("Time"),
                        "race_speed":st.column_config.NumberColumn("km/h", format="%.2f"),
                        "new_pb":st.column_config.CheckboxColumn("PB"),
                        "segment_time":st.column_config.TextColumn("Segment"),
                    })

        st.markdown("""
                    -----
                    This data shows all race efforts, not just your bests that contribute to the leaderboard.
                    """)

def render_stats_tab(tab, data):

    def power_figure(data):

        c1, c2 = st.columns([6,3])
        c2.markdown("")
        c2.markdown("")
        x_metric = c2.selectbox("X-Axis Value", options=["W/kg", "Watts",])
        y_metric = c2.selectbox("Y-Axis Value", options=["Speed", "Time",])
        selected_routes = c2.multiselect("Route", options=data[["route"]].unique().sort(pl.col("route"))["route"].to_list())
        selected_gender_category = c2.selectbox("Gender Category", options=["Mixed", "Womens", "Mens"])
        selected_power_category = c2.selectbox("Power Category", options=["All", "A", "B", "C", "D"])

        fig = px.scatter(
            data
                .filter(pl.col("route").is_in(selected_routes) if len(selected_routes)>0 else True)
                .filter(pl.col("gender_category")==selected_gender_category if len(selected_gender_category)>0 else True)
                .filter(pl.col("power_category")==selected_power_category if selected_power_category!="All" else True)
                .sort(pl.col("power_category")),
            x="watts_average" if x_metric=="Watts" else "wkg_average",
            y="race_seconds" if y_metric=="Time" else "race_speed",
            color="power_category",
            opacity=0.4,
            labels={
                "watts_average": "Average Power (W)",
                "wkg_average": "Average Power (W/kg)",
                "race_seconds": "Time (seconds)",
                "race_speed": "Speed (km/h)",
                "power_category": "Power Category"
            },
            )
        
        c1.plotly_chart(fig)



    with tab:

        summaries_data = data.filter(pl.col("gender_category")=="Mixed")
        distance = sum(summaries_data["route_length"])
        hours = sum(summaries_data["race_seconds"])/3600
        kwhs = sum((summaries_data["watts_average"]/1000) * (summaries_data["race_seconds"]/3600))
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Riders 🚴‍♂️", f"{summaries_data[["rider_id"]].unique().shape[0]:,.0f}", border=True)
        c2.metric("Efforts 🏁", f"{summaries_data.shape[0]:,.0f}", border=True)
        c3.metric("PBs 🏆", f"{sum(summaries_data["new_pb"]):,.0f}", border=True)
        
        c1.metric("Distance 🌍", f"{distance:,.0f} km", border=True)
        c2.metric("Hours ⏱️", f"{hours:,.0f}", border=True)
        #c3.metric("Speed 🚀", f"{distance/hours:,.1f} km/h", border=True)
        #c3.metric("Elevation 🏔️", f"{sum(summaries_data["route_elevation"]):,.0f} m", border=True)
        c3.metric("Everests Climbed 🏔️", f"{sum(summaries_data["route_elevation"])/8848:,.2f}", border=True)
        
        c1.metric("Energy Generated ⚡", f"{kwhs:,.0f} kW/h", border=True)
        c2.metric("Calories Burned 🔥", f"{kwhs * 860.420 / 0.24:,.0f}", border=True)
        c3.metric("Pizza Slices 🍕", f"{kwhs * 860.420 / 0.24 / 266:,.0f}", border=True)
        
        power_figure(data)