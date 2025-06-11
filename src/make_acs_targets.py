import src.wrangle.csv as wgl
from pathlib import Path

# this is basically a starter for what will eventually
# be fleshed out into a public table.

# paths
project_path = Path.cwd()
data_path = project_path / "data"
csv_path = data_path / "raw" / "census-api.csv"

# wrangle csv
lf = wgl.csv_wrangle(csv_path)

# write parquet
parq_path = data_path / "targets" / "acs-api.parquet"
lf.sink_parquet(parq_path)
