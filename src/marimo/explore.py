import marimo

__generated_with = "0.14.17"
app = marimo.App(width="columns")


@app.cell(column=0)
def _(mo):
    mo.md(r"""## Exploration Side""")
    return


@app.cell
def _(needs_refresh, pull_endpoints):
    # main logic
    # basically grabbing everything here for testing - but normally set this up so its once a year sort of a deal
    refresh_wait = "1w"
    active_rows = False

    source = needs_refresh(refresh=refresh_wait, active_only=active_rows).collect()

    source.head()

    endpoints = pull_endpoints(source)
    return (endpoints,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""goal: for today - I want to refine and simplify some of the etl.""")
    return


@app.cell
def _(endpoints, fetch_data_from_endpoints):
    lf = fetch_data_from_endpoints(endpoints)
    lf.head().collect()
    return (lf,)


@app.cell(hide_code=True)
def _(lf, mo, pl):
    mo.md("# standardize some text")

    lf.with_columns(
        pl.col(["universe", "concept", "measure","value_type"]).str.to_lowercase(),
    ).head().collect()
    return


@app.cell
def _():
    return


@app.cell(column=1, hide_code=True)
def _(mo):
    mo.md(r"""## dependencies and ideas side""")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from datetime import datetime as dt
    import logging
    from acs.etl import needs_refresh
    from acs.api import fetch_data_from_endpoints
    from dataops.socrata.data import pull_endpoints
    return fetch_data_from_endpoints, mo, needs_refresh, pl, pull_endpoints


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    some standardization might be nice here - and then we can push that back up into the library.
    I think some of the builder optimizations could be to pull back into the main parts of the library?

    thinking about the "ETL" a bit too. so there is the "process control" table which will need to be managed, there is 
    also  the rows of data. This is going to run, maybe once a year - I dunno if the upsert actually makes any sense.
    We can manage and even "ETL" the process control table with the rolling 5 years, and just replace the data that comes out at the end of it.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    so really this becomes a few different domains, we have:  
    - data flow from external to ODP  
    - Process control reading  
    - Process control management and etl
    """
    )
    return


@app.cell
def _(mo):
    mo.mermaid(
        """
    graph TD
        A[ACS Data Flow App] --> B(read ODP Process Control)
        B --> C{Update or Read as-is ODP Process Control}
        C -->D[Read as-is]
        C -->E[Rolling 5 year Endpoints]
        """
    )
    return


if __name__ == "__main__":
    app.run()
