from dataops.acs import variables as var
from dataops.acs import pull as pull
import polars as pl
from dotenv import load_dotenv
import os

df = pl.read_parquet("data/targets/acs-api.parquet")

domains, endpoints = pull.get_domain_and_endpoints(df)

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")

data = pull.fetch_all_data(domains, endpoints, format= "long", token = API_TOKEN)

var01 = var.read_acs_var_html(2023, 1)
var05 = var.read_acs_var_html(2023, 5)

# hmm faster to have a list of wide dfs and loop through and
# fire off the data somewhere?
# or
# keep em long with the call_id?
# I guess it matter if the data is always gonna be one stupidly wide row


# assume 1 domain - multi endpoints

for url, data in data.items():
    print(f"\nData from {url}:")
    print(data)

testcase = data["https://api.census.gov/data/2023/acs/acs1/subject?get=group(S2301)&ucgid=0400000US09"]

#TODO join testcase up to var names