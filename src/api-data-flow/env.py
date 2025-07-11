from dataops.models import ApplicationSettings
import logging


# TODO add some more utitlity around this
def load_settings():
    logging.info("Attempting to load environmental variables.")
    settings = ApplicationSettings()
    logging.info("Environmental variables successfully loaded.")

    return settings
