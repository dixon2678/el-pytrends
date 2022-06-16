import requests
import pandas as pd
import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from pytrends.request import TrendReq

gcp_json_credentials_dict = json.loads(os.environ['creds'])
credentials = service_account.Credentials.from_service_account_info(gcp_json_credentials_dict)

# Creds are supplied through Airflow's environment variables

class extractLoad:

    # Fetch prices from Binance API

    """
    Fetches all information on every available pairs on Binance 
    (cryptocurrency trading platform) as json
    Converts to DataFrame with pandas built-in read_json

    Input : None
    Output : DataFrame
    """

    def fetch_api(self):
    	trends = TrendReq()
	trends.build_payload(kw_list=['/m/0vpj4_b'], timeframe='today 5-y')
	trends_df = trends.interest_over_time()
	trends_df.rename(columns={'/m/0vpj4_b' : 'score'}, inplace=True)
	return trends_df
    


    # Load to Database

    """
    Loads DataFrame to BigQuery as a table

    Input : DataFrame
    Output : None
    """

    def load_bigquery(self, dataframe):
        print("Data Loaded")
        table_id = 'final-347314.main.binance_api'
        client = bigquery.Client(credentials=credentials)
        client.load_table_from_dataframe(dataframe, table_id)
