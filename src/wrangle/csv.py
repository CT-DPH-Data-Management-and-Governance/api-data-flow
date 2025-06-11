import polars as pl
from pathlib import Path


def csv_wrangle(target_file: Path) -> pl.LazyFrame:
    """Wrangle starter .csv for creating api endpoints and features.

    Args:
        target_file: The path to the file to be processed.

    Returns:
        A polars LazyFrame.

    Raises:
        FileNotFoundError: If `target_file` does not exist.
        IOError: For general I/O related errors.

    """
    if not Path.exists(target_file):
        raise FileNotFoundError(f"File path does not exist: {target_file}")

    if not target_file.suffix == ".csv":
        raise IOError(f"Expected a csv file. Found file type: {target_file.suffix}")

    # regex patterns
    group_pattern = r"\((.*)\)"
    ucgid_pattern = r"&ucgid=(.*)$"

    targets = pl.scan_csv(target_file).with_columns(
        pl.col("url").str.slice(28, 4).cast(pl.Int16).alias("year"),
        pl.col("url").str.slice(40, 1).cast(pl.Int16).alias("acs_type"),
        pl.col("url").str.extract(group_pattern).alias("group"),
        pl.col("url").str.extract(ucgid_pattern).alias("ucgid"),
    )

    return targets
