import pandas as pd
import streamlit as st
import plotly.express as px

import plugins.utils.sqlconn as sql
from plugins.conf.config import factcolumns
from plugins.utils import vizquery as q
from plugins.conf import config


def make_visualization(query, column, plot_type="agg"):
    """
    Helper function to plot bar chart.
    :param String query: query
    :param List(str) column: list of column
    :param String plot_type: accepted value 'agg' and 'party'
    :returns: None.
    """
    if plot_type not in ("agg", "party"):
        raise ValueError("Only accepted type is agg and party")

    engine = sql.get_sql_conn(db_uri=config.mounted_local_db_uri)
    try:
        df = pd.read_sql_query(query, engine)
        if plot_type == "agg":
            df["country_orientation"] = df.country_orientation.astype("category")
            x = "country_orientation"
            y = "count_country_orientation"
            title = f"<b>Aggregate Countrywise Orientation on : {column}</b>"

        elif plot_type == "party":
            df["country_orientation"] = df.country_orientation.astype("category")
            x = "party"
            y = "country_orientation"
            title = f"<b>Country and Party Orientation on {column}</b>"

        fig_party = px.bar(
            df,
            x=x,
            y=y,
            color="Country",
            title=title,
        )

        st.plotly_chart(fig_party, use_container_width=True)

    except Exception as e:
        pass


def main():

    # Lookup data for country
    @st.cache
    def load_lookup_data():
        engine = sql.get_sql_conn(db_uri=config.mounted_local_db_uri)
        lookup_df = pd.read_sql_table("country_lookup_table", engine)
        return lookup_df

    lookup_df = load_lookup_data()

    st.header("CHES2019 Analysis :wave:")
    radio_select = [
        "Countrywise aggregate information",
        "Country and Partywise information",
    ]
    plot_type = st.radio("Make Your Selection", radio_select)

    # Fetch distinct country from the datasets.
    countries = lookup_df["Country"].drop_duplicates()
    countries = tuple(countries.to_list())

    if plot_type == radio_select[0]:
        column = st.sidebar.selectbox("Select column for the orientation:", factcolumns)

        select_all_countries = st.checkbox(
            "Select ALL for aggregate info on ALL countries"
        )

        if select_all_countries:
            countries = countries
            query = q.query_for_countrywise_agg_orientation.format(column, countries)

            make_visualization(query, column)

        multi_select_countries = st.checkbox(
            "Select specific countries for aggregate info "
        )

        if multi_select_countries:
            countries = st.multiselect(
                "Select countries", countries, default="United Kingdom"
            )
            if len(countries) > 1:
                countries = tuple(countries)
            else:
                try:
                    c = countries[0]
                    countries = f"('{c}')"
                except Exception as e:
                    st.write("select atleast one country")

            query = q.query_for_countrywise_agg_orientation.format(column, countries)
            make_visualization(query, column)

    if plot_type == radio_select[1]:
        column = st.sidebar.selectbox("Select column for the orientation:", factcolumns)

        countries = st.multiselect(
            "Select countries", countries, default="United Kingdom"
        )
        if len(countries) > 1:
            countries = tuple(countries)
        else:
            try:
                c = countries[0]
                countries = f"('{c}')"
            except Exception as e:
                st.write("select atleast one country")

        query = q.query_for_country_party_orientation.format(column, countries)
        make_visualization(query, column, plot_type="party")


if __name__ == "__main__":
    main()
