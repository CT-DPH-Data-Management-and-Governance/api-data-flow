from dataops.models import ApplicationSettings
from dotenv import load_dotenv
import os
import sys
import logging


# TODO add some more utitlity around this
def load_settings():
    settings = ApplicationSettings()

    return settings


# environmental variables/secrets
logging.info("Attempting to load environmental variables.")
if load_dotenv():
    CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
    TABLE_SOURCE = os.getenv("TABLE_SOURCE")
    TABLE_TARGET = os.getenv("TABLE_TARGET")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    TOKEN = os.getenv("TOKEN")
    DOMAIN = os.getenv("DOMAIN")
    logging.info("Environmental variables successfully loaded.")
else:
    logging.critical("Environmental variables cannot be loaded - exiting process.")
    sys.exit(1)
