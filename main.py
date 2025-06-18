from dataops.models import CensusAPIEndpoint
import polars as pl
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")


def fetch_data(urls: list[str]) -> pl.DataFrame:
    all_frames = []

    for url in urls:
        df = CensusAPIEndpoint.from_url(url).fetch_tidy_data().lazy()
        print(df.head())
        all_frames.append(df)

    return pl.concat(all_frames)


def local_urls() -> list:
    path = Path.cwd() / "data" / "raw" / "census-api.csv"
    return pl.read_csv(path).to_series().to_list()


def main():
    print("pulling from census apis")
    lf = fetch_data(local_urls())
    lf.sink_parquet("whole-game.parquet")


if __name__ == "__main__":
    main()
