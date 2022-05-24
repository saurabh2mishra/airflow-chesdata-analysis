import numpy as np
import pandas as pd
import tabula

import utils.sqlconn as sc


def get_and_staged_data(path, table_name, db_uri, file_type="csv"):
    """
    Accept path and returns a pandas dataframe.
    :param Path path: database file or uri
    :param String file_name: type of file.
    :param String file_type: type of file.
    :return: Pandas dataframe
    """
    if file_type not in ["csv"]:
        raise ValueError("Only accepts csv uri/file")

    data = pd.read_csv(path)

    if table_name == "party":
        data.loc[:, ["country_abbrev"]] = data.loc[:, ["country_abbrev"]].ffill()

    sc.write_to_sql(data, table_name, db_uri)


def _generate_country_abbr_and_party(path, table_name, db_uri):
    df = pd.read_csv(path)
    sc.write_to_sql(df, table_name, db_uri)


def generate_country_abbr_and_party(path, table_name, db_uri):
    """
    This function takes list of dataframe generated from tabula pdf read and
    :param Path path: database file or uri
    :param String file_name: type of file.
    :return: None
    """
    df = tabula.read_pdf(path, pages="all", multiple_tables=True)
    if len(df) < 1:
        raise ValueError("Expect not a blank list")

    # Rendering country abbr info from the pdf
    country_abbr_df = df[0].dropna()
    c1_schema = {
        "Country ID": "CountryID",
        "Country": "CountryAbbr",
        "Country.1": "Country",
    }
    c2_schema = {
        "Country ID.1": "CountryID",
        "Country.2": "CountryAbbr",
        "Country.3": "Country",
    }
    c1 = country_abbr_df.iloc[:, 0:3].rename(columns=c1_schema)
    c2 = country_abbr_df.iloc[:, 3:].rename(columns=c2_schema)
    country_abbr = pd.concat([c1, c2], ignore_index=True)
    country_abbr["CountryID"] = country_abbr["CountryID"].apply(np.int64)

    sc.write_to_sql(country_abbr, table_name, db_uri)
