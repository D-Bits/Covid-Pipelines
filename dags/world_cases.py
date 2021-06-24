from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import requests


default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 5, 17),
    "retries": 1,
}

@dag(default_args=default_args, schedule_interval="@daily")
def world_cases():

    @task()
    def extract():

        res = requests.get("https://disease.sh/v3/covid-19/countries")
        json_data = res.json()

        return json_data

    @task()
    def transform(data):

        df = pd.DataFrame(data)
        undefined = df['undefined']
        country_info = df['countryInfo']
        df['dates'] = pd.to_datetime(df['updated'] / 1000, unit="s")
        dropped_fields = [undefined, country_info]
        cleaned_data = df.drop(['countryInfo', 'updated', 'undefined'], axis=1) 

    @task()
    def load(cleaned_data):

        pass
