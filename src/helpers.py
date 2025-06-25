import polars as pl
from dotenv import load_dotenv
import os
from sodapy import Socrata

# environmental variables/secrets
if load_dotenv():
    CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
    TABLE_SOURCE = os.getenv("TABLE_SOURCE")
    TABLE_TARGET = os.getenv("TABLE_TARGET")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    TOKEN = os.getenv("TOKEN")
    DOMAIN = os.getenv("DOMAIN")


def current_source() -> pl.DataFrame:
    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        urls = client.get_all(TABLE_SOURCE)

        return pl.DataFrame(urls)


def current_target() -> pl.DataFrame:
    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        urls = client.get_all(TABLE_TARGET)

        return pl.LazyFrame(urls).head().collect()
