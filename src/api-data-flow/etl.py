from sodapy import Socrata
from fetch import fetch_source

import polars as pl


def only_new(cutoff: dt.date = CUTOFF):
    df = fetch_source()
    new = df.with_columns(pl.col("date_last_pulled").str.to_datetime()).filter(
        (pl.col("date_last_pulled").le(cutoff)) & (pl.col("active").eq("1"))
    )
    return new


def update_source(source: pl.DataFrame):
    new = source.with_columns(pl.lit(TODAY).alias("date_last_pulled")).to_dicts()

    with Socrata(DOMAIN, TOKEN, USERNAME, PASSWORD) as client:
        client.upsert(TABLE_SOURCE, new)
