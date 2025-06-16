path <- fs::path_wd("data", "variables")

acs_2023_acs1 <- tidycensus::load_variables(year = 2023, dataset = "acs1")
acs_2023_acs5 <- tidycensus::load_variables(year = 2023, dataset = "acs5")
tidycensus::load

nanoparquet::write_parquet(
  acs_2023_acs1,
  fs::path(path, "acs_2023_acs1", ext = "parquet")
)

nanoparquet::write_parquet(
  acs_2023_acs5,
  fs::path(path, "acs_2023_acs5", ext = "parquet")
)
