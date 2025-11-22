import streamlit as st
import polars as pl


def filters(tab, data):
    st.write("  ")
    c1, c2, c3 = st.columns([2, 2, 5])
    st.write("  ")
    rider_search = st.container()
    st.write("  ")
    st.write("  ")

    category = c1.selectbox(
        "Category", options=["A", "B", "C", "D"], key=f"category_{tab}"
    )
    data_filtered = data.filter(pl.col("power_category") == category)

    gender = c2.selectbox("Gender", options=["Mixed", "Womens"], key=f"gender_{tab}")
    data_filtered = data_filtered.filter(pl.col("gender_category") == gender)

    riders = c3.multiselect(
        "Riders",
        options=data_filtered.sort(pl.col("rider"))[["rider"]].unique(),
        key=f"riders_{tab}",
    )

    data_filtered = (
        data_filtered.filter(pl.col("rider").is_in(riders))
        if len(riders) > 0
        else data_filtered
    )

    return data_filtered


def render_standings_tab(tab, data):
    with tab:
        data_filtered = filters(tab, data)

        st.dataframe(
            data_filtered[
                [
                    "rank",
                    "rider",
                    "race_count",
                    "score",
                    "position_points",
                    "segment_bonuses",
                    "pb_bonuses",
                ]
            ],
            column_config={
                "rank": st.column_config.NumberColumn("Pos.", width=16),
                "rider": st.column_config.TextColumn("Rider"),
                "race_count": st.column_config.NumberColumn("Races (count)"),
                "score": st.column_config.NumberColumn("Points"),
                "position_points": st.column_config.NumberColumn("Positon Points"),
                "segment_bonuses": st.column_config.NumberColumn("FTS Bonuses"),
                "pb_bonuses": st.column_config.NumberColumn("PB Bonuses"),
            },
        )


def render_race_results_tab(tab, data):
    with tab:

        c1, c2, c3, c4, _ = st.columns([2,2,2,2,4])
        c1.selectbox("Round", options = ["All"] + data[["round_id"]].unique().sort("round_id")["round_id"].to_list())

        data_filtered = filters(tab, data)

        st.dataframe(
            data_filtered,
            column_config={
                "rank": st.column_config.NumberColumn("Rank"),
                "category": st.column_config.TextColumn("Category"),
                "rider": st.column_config.TextColumn("Rider"),
                "gender": st.column_config.TextColumn("Gender"),
                "time_seconds": st.column_config.NumberColumn("Time"),
                "gap_seconds": st.column_config.NumberColumn("Gap"),
                "category_gap_seconds": st.column_config.NumberColumn("Gap"),
                "gender_gap_seconds": st.column_config.NumberColumn("Gap"),
                "category_gender_gap_seconds": st.column_config.NumberColumn("Gap"),
                "new_pb": st.column_config.CheckboxColumn("PB"),
            },
        )
