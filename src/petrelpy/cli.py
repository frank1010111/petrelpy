"""Command line tool for working with Petrel input and output formats."""
from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import click
import pandas as pd

from petrelpy.petrel import (
    export_perfs_ev,
    export_perfs_prn,
    export_vol,
    get_raw_table,
    read_production,
)
from petrelpy.wellconnection import (
    COL_NAMES_TRAJECTORY,
    get_trajectory_columns,
    process_well_connection_file,
)

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    """Command line tool for working with Petrel input and output formats."""


@cli.command()
@click.argument(
    "input", type=click.Path(exists=True), default=sys.stdin
)  # help="IHS spreadsheet file"
@click.option(
    "-o",
    "--output",
    type=click.Path(writable=True),
    help="Petrel vol file, defaults to input file location and name with .vol extension",
)
@click.option(
    "-y",
    "--yearly",
    is_flag=True,
    default=False,
    help="whether to aggregate production by year, by default False",
)
@click.option(
    "-z",
    "--zip",
    is_flag=True,
    default=False,
    help="whether to unzip the file, by default False",
)
def production(input: click.Path, output: click.Path, yearly: bool, zip: bool):
    """Convert IHS production spreadsheet to Petrel vol format.

    This allows production to be easily imported into Petrel.
    """
    if output is None:
        output = Path(input).with_suffix(".vol")
        click.secho(f"output: {str(output)}", fg="green")

    if zip:
        with ZipFile(input) as f:
            spreadsheet_name = f"_{'Yearly' if yearly else 'Monthly'} Production.csv"
            inside_zip = input.with_suffix(spreadsheet_name)
            wells = read_production(f.open(inside_zip), yearly)
    else:
        wells = read_production(input, yearly)
    export_vol(wells, output)


@cli.command()
@click.argument("input", type=click.Path(exists=True))
@click.option(
    "-o",
    "--output",
    type=click.Path(writable=True),
    help="well property aggregate file, defaults to input file location with .csv extension",
)
@click.option(
    "-e",
    "--heel",
    type=click.Path(exists=True),
    help="csv file with well to heel measured depth. The columns needed are UWI,Name,Depth_Heel",
)
def connection(input: click.Path, output: click.Path, heel: click.Path):
    """Process well connection file to average geomodel properties.

    This gets well properties from Petrel (in an Eclipse format) into a spreadsheet.
    """
    columns = get_trajectory_columns(input)
    # click.echo(f"The columns are {columns}")
    geomodel_cols = columns.split("  ")[1:]
    all_cols = COL_NAMES_TRAJECTORY + geomodel_cols
    heel_frame = pd.read_csv(heel)
    aggregates = (
        process_well_connection_file(input, heel_frame, col_names=all_cols)
        .dropna(subset=["GRID_I"])
        .rename_axis(index="UWI")
    )
    if output is None:
        output = Path(input).with_suffix(".csv")
    aggregates.to_csv(output)


@cli.command()
@click.argument("input", type=click.Path(exists=True), nargs=-1)
@click.option(
    "-o",
    "--output",
    type=click.Path(writable=True),
    help="petrel event file with perforations, defaults to first input file with .ev extension",
)
@click.option(
    "-h",
    "--header",
    default="UNITS FIELD\n",
    help="header information to include in the event file",
)
@click.option(
    "-d",
    "--date-col",
    default="Treatment Start Date",
    help="column for perforation dates",
)
@click.option("--sheetname", default=0, help="sheet name for excel file inputs")
def perforation(
    input: tuple[click.Path],
    output: click.Path,
    header: str,
    date_col: str,
    sheetname: str | int,
):
    """Create petrel perforation file.

    Takes csv, excel, or prn files as input.
    Expected columns include: API, Treatment Start Date, start_depth, stop_depth

    Produces .ev (default) or .prn file
    """
    perforations = pd.concat(
        [get_raw_table(fname, sheetname) for fname in input]
    ).rename_axis(index="API")
    click.echo(f"{len(perforations)} reports found")
    if date_col not in perforations.columns:
        msg = f"{date_col} is not among the columns loaded from the files provided"
        raise click.BadParameter(msg, param=date_col)
    perforations["Date"] = pd.to_datetime(perforations[date_col]).dt.strftime(
        "%m.%d.%Y"
    )
    output = Path(input[0]).with_suffix(".ev") if output is None else Path(output)
    if output.suffix == ".ev":
        export_perfs_ev(perforations, output, header)
    elif output.suffix == ".prn":
        export_perfs_prn(perforations, output)
    else:
        msg = (
            f"The output file {output} does not have a supported extension.\n"
            "Supported extensions are .ev and .prn"
        )
        option_name = "output"
        raise click.BadOptionUsage(option_name, msg)
