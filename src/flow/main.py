from sodapy import Socrata

# from dataops.socrata.data import replace_data
import polars as pl
from datetime import datetime as dt
import logging
from flow.etl import fetch_endpoints, update_source, fetch_data_from_endpoints
from dataops.socrata.data import pull_endpoints
from dataops.settings.flow import AppSettings


def chunked_replace(data: pl.LazyFrame, timeout: int = 60) -> None:
    settings = AppSettings()
    target = settings.api.target.id

    chunk_max_size = 200_000  # Set your desired chunk size

    client = Socrata(
        settings.api.domain,
        settings.account.token.get_secret_value(),
        settings.account.username,
        settings.account.password.get_secret_value(),
        timeout=timeout,
    )

    # TODO convert to logging
    try:
        # Get the total row count efficiently without loading all data
        total_rows = data.select(pl.len()).collect().item()
        print(f"Total rows to process: {total_rows:,}")
        print(f"Processing in chunks of: {chunk_max_size:,} rows")

        is_first_chunk = True

        # Loop through the LazyFrame by slicing it
        for offset in range(0, total_rows, chunk_max_size):
            print(
                f"Processing rows from {offset:,} to {offset + chunk_max_size - 1:,}..."
            )

            # Get the current chunk as a LazyFrame
            chunk_lf = data.slice(offset, chunk_max_size)

            payload = chunk_lf.collect().to_dicts()

            if not payload:
                print("No more data to process.")
                break

            if is_first_chunk:
                print(f"Uploading first chunk ({len(payload)} rows) using 'replace'...")
                client.replace(target, payload)
                is_first_chunk = False  # All subsequent chunks will append
            else:
                print(f"Uploading next chunk ({len(payload)} rows) using 'upsert'...")
                client.upsert(target, payload)

        print("\n✅ Successfully uploaded all data to Socrata.")

    except Exception as e:
        print(f"\n❌ An error occurred: {e}")

    finally:
        client.close()


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
