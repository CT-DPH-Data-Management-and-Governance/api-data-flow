import polars as pl
from dataops.models import CensusAPIEndpoint
from acs.api import fetch_data_from_endpoints
from datetime import datetime as dt


# GOAL this is a quick post-hoc wrangle
# should be ported upstream to dataops

# todo: drop instanceid after used
# parse variable_id further
# split out variable name as much as possible
# b tables full parse var id
# s tables grab as much actionable meta


# assumption: data in memory
# row example:
#   {
#     "row_id": "0",
#     "instance_id": "16748936277403670050",
#     "dataset": "acs/acs1/subject",
#     "year": "2022",
#     "concept": "educational attainment",
#     "geo_id": "0400000US09",
#     "ucgid": "0400000US09",
#     "geo_name": "Connecticut",
#     "variable_id": "S1501_C04_026E",
#     "variable_name": "estimate percent male age by educational attainment population 65 years and over high school graduate or higher",
#     "value": "89.0999984741211",
#     "value_type": "estimate",
#     "full_url": {
#       "url": "https://api.census.gov/data/2022/acs/acs1/subject?get=group%28S1501%29&ucgid=0400000US09"
#     },
#     "date_pulled": "2025-07-10T00:00:00.000"
#   },


def test_endpoints():
    return [
        "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1701)&ucgid=0400000US09",
        "https://api.census.gov/data/2023/acs/acs1?get=group(B19013H)&ucgid=0400000US09",
        "https://api.census.gov/data/2023/acs/acs1?get=group(B25088)&ucgid=0400000US09",
        "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S2301)&ucgid=0400000US09",
    ]


var_test = (
    "https://api.census.gov/data/2023/acs/acs1?get=group(B19013H)&ucgid=0400000US09"
    # "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S2301)&ucgid=0400000US09"
)

tv = CensusAPIEndpoint.from_url(var_test)

tv.url_no_key
tv.variable_url
tv.fetch_all_variable_labels()
tv.fetch_variable_labels()


# approx main pre platfrom etl
DATE_PULLED = dt.now().strftime("%Y-%m-%d %H:%M:%S")

lf = fetch_data_from_endpoints(test_endpoints())
data = lf.with_columns(pl.lit(DATE_PULLED).alias("date_pulled")).collect()

# push this all upstream to lib

var_cols = ["row_id", "variable_id", "variable_name", "value"]

b_table_split_expr = (
    pl.col("variable_id")
    .str.split_exact(by="_", n=1)
    .struct.rename_fields(["table_id", "line_id"])
    .alias("parts")
)
s_table_split_expr = (
    pl.col("variable_id")
    .str.split_exact(by="_", n=2)
    .struct.rename_fields(["table_id", "column_id", "line_id"])
    .alias("parts")
)

table_type_expr = pl.col("variable_id").str.slice(0, 1).alias("table_type")
common_var_meta_expr = (
    pl.col("table_id").str.slice(0, 1).alias("table_type"),
    pl.col("table_id").str.slice(1, 2).alias("table_subject_id"),
    pl.col("table_id").str.slice(3, 3).alias("subject_table_number"),
    pl.col("table_id").str.slice(6).alias("table_id_suffix"),
)

common_line_expr = (
    pl.col("line_id").str.slice(0, 3).alias("line_number").str.to_integer(),
    pl.col("line_id").str.slice(3).alias("line_suffix"),
)


core = data.lazy().select(var_cols).with_columns(table_type_expr)

b_vars = (
    core.filter(pl.col("table_type").eq("B"))
    .with_columns(b_table_split_expr)
    .unnest("parts")
    .with_columns(common_var_meta_expr)
    .with_columns(common_line_expr)
    .with_columns(
        pl.lit(None).cast(pl.Int64).alias("column_number"),
        pl.lit(None).cast(pl.String).alias("column_id"),
        pl.col(pl.String).replace("", None),
    )
)

s_vars = (
    core.filter(pl.col("table_type").eq("S"))
    .with_columns(s_table_split_expr)
    .unnest("parts")
    .with_columns(
        pl.col("column_id").str.slice(-2).str.to_integer().alias("column_number"),
    )
    .with_columns(common_var_meta_expr)
    .with_columns(common_line_expr)
    .with_columns(pl.col(pl.String).replace("", None))
)

b_vars.head().collect()
s_vars.head().collect()

pivot_vars = [
    "variable_id",
    "variable_name",
    "value",
    "line_number",
    "line_suffix",
    "table_id",
]

# this looks pretty good  so far
b_vars.select(pivot_vars).collect().pivot(
    "line_suffix", index=["line_number", "variable_name", "table_id"], values="value"
).sort(["table_id", "line_number"])


data.filter(pl.col("dataset").ne("acs/acs1/subject")).select(pl.col("concept")).unique()


# if you grab variables from the endpoint with groups
# e.g. https://api.census.gov/data/2023/acs/acs1/groups/B25088/


# you get something like

# "B25088_002M": {
#   "label": "Margin of Error!!Median selected monthly owner costs (dollars) --!!Housing units with a mortgage (dollars)",
#   "concept": "Median Selected Monthly Owner Costs (Dollars) by Mortgage Status",
#   "predicateType": "int",
#   "group": "B25088",
#   "limit": 0,
#   "predicateOnly": true,
#   "universe": "Owner-occupied housing units"
# },
# "B25088_002E": {
#   "label": "Estimate!!Median selected monthly owner costs (dollars) --!!Housing units with a mortgage (dollars)",
#   "concept": "Median Selected Monthly Owner Costs (Dollars) by Mortgage Status",
#   "predicateType": "int",
#   "group": "B25088",
#   "limit": 0,
#   "predicateOnly": true,
#   "universe": "Owner-occupied housing units"
# }

# we need to parsae the label better
# looks like  you have line_type!! concept base !! stratification
# we already get that first part, don't really need the middle- because covered by base.

# what about s tables?


# "S2301_C03_021EA": {
#   "label": "Annotation of Estimate!!Employment/Population Ratio!!Population 20 to 64 years",
#   "concept": "Employment Status",
#   "predicateType": "string",
#   "group": "S2301",
#   "limit": 0,
#   "predicateOnly": true
# },
# "S2301_C01_020EA": {
#   "label": "Annotation of Estimate!!Total!!Population 16 years and over!!RACE AND HISPANIC OR LATINO ORIGIN!!White alone, not Hispanic or Latino",
#   "concept": "Employment Status",
#   "predicateType": "string",
#   "group": "S2301",
#   "limit": 0,
#   "predicateOnly": true
# },

# top one line_type !! loosely concept base? !! stratification
# bottom one - hmmm -    line_type !! Total!! stratication !! race/eth modifier !! race/eth

# s tables might be messy

# b tables might be fine
