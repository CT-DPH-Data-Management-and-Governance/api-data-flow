from dataops.socrata.data import fetch_data
import polars as pl


def snapshot_source():
    raw = fetch_data()

    orig_endpoints = (
        raw.with_columns(
            pl.col("endpoint").struct.unnest().alias("endpoint"),
            pl.col("row_id").cast(pl.UInt32),
        )
        .sort(pl.col("row_id"))
        .collect()
    )

    return orig_endpoints


# todo common snapshot manipulations like with ids etc...
# validation
# dedupe?
