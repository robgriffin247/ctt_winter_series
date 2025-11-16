import streamlit as st
import polars as pl 

def render_standings_tab(tab, data):
    with tab:
        # Layout
        st.write("  ")
        c1, c2, c3 = st.columns([2,2,5])
        st.write("  ")
        rider_search = st.container()
        st.write("  ")
        st.write("  ")
        table_container = st.container()
        st.divider()
        st.markdown("**Points = 50 + Position - FTS Bonus - PB Bonus*")

        # Backend
        category = c1.selectbox("Category", options=["All", "A", "B", "C", "D"])
        data_filtered = data.filter(pl.col("category")==category) if category!="All" else data

        gender = c2.selectbox("Gender", options=["Mixed", "Womens", "Mens"])
        data_filtered = data_filtered.filter(pl.col("gender")=="F" if gender=="Womens" else pl.col("gender")=="M") if gender!="Mixed" else data_filtered
        
        data_filtered = data_filtered.with_row_index("rank", offset=1)

        riders = rider_search.multiselect("Riders", 
                                 options=data_filtered.sort(pl.col("rider"))[["rider"]].unique(), 
                                 key="standings_riders")
        data_filtered = data_filtered.filter(pl.col("rider").is_in(riders)) if len(riders)>0 else data_filtered
        

        columns = ["rank", "rider", "category", "gender"]

        if category=="All" and gender=="Mixed":
            columns += ["overall_score"]

        if category!="All" and gender=="Mixed":
            columns += ["category_score"]

        if category=="All" and gender!="Mixed":
            columns += ["gender_score"]

        if category!="All" and gender!="Mixed":
            columns += ["category_gender_score"]


        table_container.dataframe(
            data_filtered[columns],
            column_config={
                "rank":st.column_config.NumberColumn("Rank", width=16),
                "category":st.column_config.TextColumn("Category"),
                "rider":st.column_config.TextColumn("Rider"),
                "gender":st.column_config.TextColumn("Gender"),
                "overall_score":st.column_config.NumberColumn("Points"),
                "category_score":st.column_config.NumberColumn("Points"),
                "gender_score":st.column_config.NumberColumn("Points"),
                "category_gender_score":st.column_config.NumberColumn("Points"),
            })




def render_race_results_tab(tab, data):
    with tab:
        # Layout
        st.write("  ")
        c1, c2, c3 = st.columns([2,2,5])
        st.write("  ")
        rider_search = st.container()
        st.write("  ")
        st.write("  ")
        table_container = st.container()
        st.divider()

        # Backend
        category = c1.selectbox("Category", options=["All", "A", "B", "C", "D"], key="results_category")
        data_filtered = data.filter(pl.col("category")==category) if category!="All" else data

        gender = c2.selectbox("Gender", options=["Mixed", "Womens", "Mens"], key="results_gender")
        data_filtered = data_filtered.filter(pl.col("gender")=="F" if gender=="Womens" else pl.col("gender")=="M") if gender!="Mixed" else data_filtered
        
        data_filtered = data_filtered.with_row_index("rank", offset=1)

        riders = rider_search.multiselect("Riders", 
                                 options=data_filtered.sort(pl.col("rider"))[["rider"]].unique(), 
                                 key="results_riders")
        data_filtered = data_filtered.filter(pl.col("rider").is_in(riders)) if len(riders)>0 else data_filtered
        

        columns = ["rank", "round_id", "event_id", "rider", "category", "gender", "time_seconds", ]

        if category=="All" and gender=="Mixed":
            columns += ["gap_seconds"]

        if category!="All" and gender=="Mixed":
            columns += ["category_gap_seconds"]

        if category=="All" and gender!="Mixed":
            columns += ["gender_gap_seconds"]

        if category!="All" and gender!="Mixed":
            columns += ["category_gender_gap_seconds"]

        columns += ["new_pb"]

        table_container.dataframe(
            data_filtered[columns],
            column_config={
                "rank":st.column_config.NumberColumn("Rank"),
                "category":st.column_config.TextColumn("Category"),
                "rider":st.column_config.TextColumn("Rider"),
                "gender":st.column_config.TextColumn("Gender"),
                "time_seconds":st.column_config.NumberColumn("Time"),
                "gap_seconds":st.column_config.NumberColumn("Gap"),
                "category_gap_seconds":st.column_config.NumberColumn("Gap"),
                "gender_gap_seconds":st.column_config.NumberColumn("Gap"),
                "category_gender_gap_seconds":st.column_config.NumberColumn("Gap"),
                "new_pb":st.column_config.CheckboxColumn("PB"),
            })



