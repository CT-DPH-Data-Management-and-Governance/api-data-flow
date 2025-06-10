import polars as pl
import requests

# test case
resp = requests.get(
    "https://api.census.gov/data/2023/acs/acs1/subject?get=group(S1701)&ucgid=0400000US09"
)

resp.raise_for_status()

json = resp.json()

df = pl.DataFrame(
    {
        "id": json[0],
        "values": json[1],
    }
).with_columns(pl.when(pl.col("values" == pl.lit("null"))).then(None))


# wrangle targets test case
targets = pl.read_csv("data/targets/census-api.csv").with_columns(
    pl.col("url").str.slice(28, 4).cast(pl.Int16).alias("year"),
    pl.col("url").str.slice(40, 1).cast(pl.Int16).alias("acs_type"),
)


def main():
    print("Hello from api-data-flow!")


if __name__ == "__main__":
    main()
