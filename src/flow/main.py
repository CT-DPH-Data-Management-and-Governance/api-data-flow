from sodapy import Socrata

# from dataops.socrata.data import replace_data
import polars as pl
from datetime import datetime as dt
import logging
from flow.etl import fetch_endpoints, update_source, fetch_data_from_endpoints
from dataops.socrata.data import pull_endpoints
from dataops.settings.flow import AppSettings


def main():
    """Entrypoint into the census acs api data flow app."""

    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
    )

    settings = AppSettings()

    logging.info("Fetching endpoint data.")
    source = fetch_endpoints().collect()

    if not source.is_empty():
        endpoints = pull_endpoints(source)
        logging.info("Endpoint data lazily loaded.")

        data = fetch_data_from_endpoints(endpoints)

        data = data.with_columns(
            pl.lit(dt.now()).dt.to_string(format="%Y-%m-%d").alias("date_pulled"),
        )

        logging.info("Pushing data to the ODP.")

        # replace
        target = settings.api.target.id

        dict_data = data.lazy().collect().to_dicts()

        with Socrata(
            settings.api.domain,
            settings.account.token.get_secret_value(),
            settings.account.username,
            settings.account.password.get_secret_value(),
            timeout=180,
        ) as client:
            client.replace(target, dict_data)

        logging.info("Data successfully pushed to the ODP.")

        logging.info("Updating Source metadata.")
        update_source(source)
        logging.info("Metadata successfully updated.")

    else:
        logging.info("No eligible endpoints - ending process.")


if __name__ == "__main__":
    main()
