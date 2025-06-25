from dataops.models import CensusAPIEndpoint
import polars as pl
from dotenv import load_dotenv
from datetime import datetime
import os
from sodapy import Socrata

# environmental variables/secrets
if load_dotenv():
    CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
    TABLE_SOURCE = os.getenv("TABLE_SOURCE")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    TOKEN = os.getenv("TOKEN")
    DOMAIN = os.getenv("DOMAIN")
