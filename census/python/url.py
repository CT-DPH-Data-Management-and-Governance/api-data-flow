from dataops import models as model
from pathlib import Path
import polars as pl


# pretend we're reading from odp table
targetfile = Path.cwd() / "data" / "raw" / "census-api.csv"
targets = pl.read_csv(targetfile)


# parse the URLS


def safe_parse_from_url(url: str) -> model.CensusAPIEndpoint | None:
    """
    Tries to parse a URL into a CensusAPIEndpoint object.
    Returns None if parsing fails.
    """
    try:
        return model.CensusAPIEndpoint.from_url(url)
    except ValueError:
        return None


# the basic idea
# df_with_objects = targets.with_columns(
#     pl.col("url").map_elements(
#         safe_parse_from_url,
#         return_dtype=pl.Object # The output is a Python object
#     ).alias("endpoint_object")
# )


# read one for now
x = model.CensusAPIEndpoint.from_url(targets.head(1).select("url").item())
df = x.fetch_data_to_polars()


# join vars
