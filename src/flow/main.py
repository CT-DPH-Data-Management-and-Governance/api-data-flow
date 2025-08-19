import polars as pl
from datetime import datetime as dt
import logging
from acs.etl import needs_refresh
from acs.api import fetch_data_from_endpoints
from dataops.socrata.data import pull_endpoints


def main():
    """Entrypoint into the census acs api data flow app."""

    # for debug and testing
    # refresh the data how long after last pull?
    refresh_wait = "1w"

    # active rows only, or everything (for testing)
    active_rows = False

    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
    )

    date_pulled = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info("Fetching endpoint data.")
    source = needs_refresh(refresh=refresh_wait, active_only=active_rows).collect()

    endpoint_ids = source.select(
        pl.col("endpoint").struct.unnest().alias("endpoint"),
        pl.col("row_id").cast(pl.Int32).alias("socrata_endpoint_id"),
    ).lazy()

    if not source.is_empty():
        endpoints = pull_endpoints(source)

        # TODO join breaks because of html encoding - fix
        # TODO add "question" id
        lf = fetch_data_from_endpoints(endpoints).join(
            endpoint_ids, on="endpoint", how="left"
        )
        logging.info("Endpoint data lazily loaded.")

        # data = lf.with_columns(pl.lit(date_pulled).alias("date_pulled")).collect()

        # logging.info("Pushing data to the ODP.")
        # replace_data(data)
        # logging.info("Data successfully pushed to the ODP.")

        # logging.info("Updating Source metadata.")
        # update_source(source)
        # logging.info("Metadata successfully updated.")

    else:
        logging.info("No eligible endpoints - ending process.")


if __name__ == "__main__":
    main()
