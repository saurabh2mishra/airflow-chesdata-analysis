# parent airflow dir
parent_dir = "/opt/airflow/data"

# uri
codebook_uri = "https://www.chesdata.eu/s/2019_CHES_codebook.pdf"
chesdata_uri = "https://www.chesdata.eu/s/CHES2019V3.csv"
questionnaire_uri = "https://www.chesdata.eu/s/CHES_UK_Qualtrics.pdf"

# files path
chesdata_path = f"{parent_dir}/CHES2019V3.csv"
chescodebook_path = f"{parent_dir}/2019_CHES_codebook.pdf"
_chescodebook_path = f"{parent_dir}/country_abbr.csv"
party_file = f"{parent_dir}/party.csv"

# files name
codebookfile = "2019_CHES_codebook.pdf"
chesdatafile = "CHES2019V3.csv"
questionnairefile = "questionnaire.pdf"

# db details
db_schema = "db.chesdata"
docker_db_uri = "sqlite:////opt/airflow/data/db.chesdata"
docker_db_path = "/opt/airflow/data/db.chesdata"
mounted_local_db_uri = "sqlite:////Users/saurabhmishra/OfficeZone/airflow-chesdata-analysis/data/db.chesdata"

# columns for visualizations - sd columns are removed.
factcolumns = (
    "eu_position",
    "eu_salience",
    "eu_dissent",
    "eu_blur",
    "eu_cohesion",
    "eu_foreign",
    "eu_intmark",
    "eu_budgets",
    "eu_asylum",
    "lrgen",
    "lrecon",
    "lrecon_salience",
    "lrecon_dissent",
    "lrecon_blur",
    "galtan",
    "galtan_salience",
    "galtan_dissent",
    "galtan_blur",
    "immigrate_policy",
    "immigrate_salience",
    "immigrate_dissent",
    "multiculturalism",
    "multicult_salience",
    "multicult_dissent",
    "redistribution",
    "redist_salience",
    "environment",
    "enviro_salience",
    "spendvtax",
    "deregulation",
    "econ_interven",
    "civlib_laworder",
    "sociallifestyle",
    "religious_principles",
    "ethnic_minorities",
    "nationalism",
    "urban_rural",
    "protectionism",
    "regions",
    "russian_interference",
    "anti_islam_rhetoric",
    "people_vs_elite",
    "antielite_salience",
    "corrupt_salience",
    "members_vs_leadership",
    "eu_econ_require",
    "eu_political_require",
    "eu_googov_require",
)
