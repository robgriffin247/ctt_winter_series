import streamlit as st
import polars as pl 

def render_standings_tab(tab, data):
    with tab:
        # Layout
        st.write("  ")
        c1, c2, c3, _ = st.columns([2,4,4,2])
        st.write("  ")
        st.write("  ")
        table_container = st.container()
        st.divider()
        st.markdown("**Points = 50 + Position Points - FTS Bonus - PB Bonus*")

        # Backend
        category = c1.selectbox("Category", options=["All", "A", "B", "C", "D"])
        data_filtered = data.filter(pl.col("category")==category) if category!="All" else data

        clubs = c2.multiselect("Clubs", options=data_filtered.sort(pl.col("club"))[["club"]].unique())
        data_filtered = data.filter(pl.col("club").is_in(clubs)) if len(clubs)>0 else data_filtered
        
        riders = c3.multiselect("Riders", options=data_filtered.sort(pl.col("rider"))[["rider"]].unique())
        data_filtered = data.filter(pl.col("rider").is_in(riders)) if len(riders)>0 else data_filtered
        
        table_container.dataframe(
            data_filtered.select(pl.all().exclude("rider_id")),
            column_config={
                "category":st.column_config.TextColumn("Cat."),
                "category_rank":st.column_config.NumberColumn("Rank"),
                "rider":st.column_config.TextColumn("Rider"),
                "club":st.column_config.TextColumn("Team"),
                "points":st.column_config.NumberColumn("Points*"),
                "position_points":st.column_config.NumberColumn("Pos."),
                "fts_bonus":st.column_config.NumberColumn("FTS"),
                "pb_bonus":st.column_config.NumberColumn("PB"),
            })


