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

    gender = c2.selectbox("Gender", options=["Mixed", "Womens", "Mens"], key=f"gender_{tab}")
    data_filtered = data_filtered.filter(pl.col("gender_category") == gender)

    riders = c3.multiselect(
        "Riders",
        options=data.sort(pl.col("rider"))[["rider"]].unique(),
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
        if data_filtered.shape[0]==0:

            st.write("No riders found - check the power and gender category is correct!")

        else:
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
                    "rank": st.column_config.NumberColumn("#", width=16),
                    "rider": st.column_config.TextColumn("Rider"),
                    "race_count": st.column_config.NumberColumn("Races"),
                    "score": st.column_config.NumberColumn("Points"),
                    "position_points": st.column_config.NumberColumn("Pos."),
                    "segment_bonuses": st.column_config.NumberColumn("FTS"),
                    "pb_bonuses": st.column_config.NumberColumn("PB"),
                },
            )

            st.markdown("""
            -----
            - *#* = rank within selected power and gender category; sorted on races completed, then points
            - *Races* = number of rounds completed and contributing to score (up to 7)
            - *Points* = total points = 50 + positions - fts points - pb points
            - *Pos.* = position points (sum of positions from contributing rounds)
            - *FTS* = total fts points from contributing rounds
            - *PB* = total PBs **in whole series**
            """)


def render_race_results_tab(tab, data):
    with tab:

        c1, c2, c3, c4, _ = st.columns([2,2,2,2,4])
        c1.selectbox("Round", options = ["All"] + data[["round_id"]].unique().sort("round_id")["round_id"].to_list())

        data_filtered = filters(tab, data)

        st.dataframe(
            data_filtered.drop(pl.col("gender_category")),
            column_config={
                "round_id": st.column_config.NumberColumn("Round"),
                "route": st.column_config.TextColumn("Route"),
                "start_datetime_utc": st.column_config.DatetimeColumn("Date/Time"),
                "rank": st.column_config.NumberColumn("Rank"),
                "power_category": st.column_config.TextColumn("Category"),
                "rider": st.column_config.TextColumn("Rider"),
                "gender": st.column_config.TextColumn("Gender"),
                "race_seconds": st.column_config.NumberColumn("Time"),
                "segment_seconds": st.column_config.NumberColumn("FTS Time"),
                "watts_average": st.column_config.NumberColumn("Watts"),
                "wkg_average": st.column_config.NumberColumn("W/kg", format="%.2f"),
                "gap_seconds": st.column_config.NumberColumn("Gap"),
                "category_gap_seconds": st.column_config.NumberColumn("Gap"),
                "gender_gap_seconds": st.column_config.NumberColumn("Gap"),
                "category_gender_gap_seconds": st.column_config.NumberColumn("Gap"),
                "new_pb": st.column_config.CheckboxColumn("PB"),
            },
        )
