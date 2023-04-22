from __future__ import annotations

import sys
from pathlib import Path
from zipfile import ZipFile

import click

from petrelpy.petrel import export_vol, read_production

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
