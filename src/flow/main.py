from dataops.socrata.data import replace_data
import polars as pl
from datetime import datetime as dt
import logging
from flow.etl import fetch_endpoints, update_source, fetch_data_from_endpoints
from dataops.socrata.data import pull_endpoints


def main():
    """Entrypoint into the census acs api data flow app."""

    now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
    )

    logging.info("Fetching endpoint data.")
    source = fetch_endpoints().collect()

    if not source.is_empty():
        endpoints = pull_endpoints(source)
        logging.info("Endpoint data lazily loaded.")

        data = fetch_data_from_endpoints(endpoints).with_columns(
            pl.lit(now).alias("date_pulled")
        )

        logging.info("Pushing data to the ODP.")
        replace_data(data)
        logging.info("Data successfully pushed to the ODP.")

        logging.info("Updating Source metadata.")
        update_source(source)
        logging.info("Metadata successfully updated.")

    else:
        logging.info("No eligible endpoints - ending process.")


if __name__ == "__main__":
    main()
