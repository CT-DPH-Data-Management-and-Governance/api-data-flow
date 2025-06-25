from dataops.models import CensusAPIEndpoint
import polars as pl
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import os

# environmental variables/secrets
load_dotenv()

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
TABLE_SOURCE = os.getenv("TABLE_SOURCE")
TABLE_TARGET = os.getenv("TABLE_TARGET")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
TOKEN = os.getenv("TOKEN")
DOMAIN = os.getenv("DOMAIN")


# grab data from endpoints
def fetch_data(urls: list[str]) -> pl.LazyFrame:
    all_frames = []

    for url in urls:
        df = CensusAPIEndpoint.from_url(url).fetch_tidy_data().lazy()
        print(df.head())
        all_frames.append(df)

    all_frames = pl.concat(all_frames)

    with_iid = all_frames.drop(["row_id", "date_pulled"]).with_columns(
        pl.struct(
            "dataset",
            "year",
            "concept",
            "geo_id",
            "ucgid",
            "geo_name",
            "variable_id",
            "variable_name",
            "value",
            "value_type",
        )
        .hash()
        .alias("instance_id")
    )

    url_id = with_iid.select(pl.col("full_url"), pl.col("instance_id")).unique()

    all_frames = (
        with_iid.drop("full_url")
        .unique()
        .join(url_id, on="instance_id")
        .with_row_index("row_id")
        .with_columns(date_pulled=datetime.now())
    )

    return all_frames


def local_urls() -> list[str]:
    path = Path.cwd() / "data" / "raw"  # / "census-api.csv"
    path = path.glob("*.csv")

    paths = pl.DataFrame()

    for file in path:
        contents = pl.read_csv(file).drop_nulls().unique()
        paths = pl.concat([paths, contents])

    return paths.to_series().to_list()


def main():
    print("pulling from census apis")
    lf = fetch_data(local_urls())
    lf.sink_parquet("whole-game.parquet")


if __name__ == "__main__":
    main()
