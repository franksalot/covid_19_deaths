import json
from logging import getLogger

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path

# SQL Filepaths
covid_deaths = (
    Path(__name__).resolve().parent / "covid_deaths.sql"
)

SCOPES = ["https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",]
SERVICE_ACCOUNT_FILE = "/Users/frank/covid_19_deaths/covid-deaths-d74588b777fb.json"

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

with open(covid_deaths, "r") as file:
    sql = file.read()

logger = getLogger(__name__)

def get_covid_deaths(
    credentials: service_account.Credentials,
) -> pd.DataFrame:
    query = sql

    # Create dataframe for migrating meterpoints
    df = pd.read_gbq(
        query,
        project_id="covid-deaths",
        dialect="standard",
        credentials=credentials,
        location="EU",
    )

    return df