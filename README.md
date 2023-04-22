# petrelpy

Scripts for getting data in and out of Petrel

Included are

- gslib: handling large GSLIB outputs
- Facies_distribution_TACC: per-facies histograms of properties from GSLIB
- tops_attributes: using tops to get attributes in and out of Petrel
- property_distribution_PetreltoArc: getting Petrel outputs into Arc-readable
  spreadsheets
- header: turning header spreadsheets into Petrel-readable header files
- convert_production_petrel: turnig production into .vol files for Petrel
- convert_perforation_petrel: turning completion spreadsheets into .ev files
- wellconnection: processing Eclipse well connection files to get per-well
  properties

## Installation

The easiest way is to use [pipx](https://pypa.github.io/pipx/):

```sh
pipx install .
```

This will put `petrelpy` on your path, and you can invoke the CLI tool with

```
$ petrelpy --help
Usage: petrelpy [OPTIONS] COMMAND [ARGS]...

  Command line tool for working with Petrel input and output formats.

Options:
  -h, --help  Show this message and exit.

Commands:
  production  Convert IHS production spreadsheet to Petrel vol format.
```

## Contributing

New Petrel-helpers are always appreciated.

To get started, clone this repo

```sh
git clone https://github.com/frank1010111/petrelpy.git
```

Install pre-commit

```sh
pipx install pre-commit
pre-commit install
```

Add a new CLI sub-command at `src/petrelpy/cli.py`! Then add tests for your new
code. These can be run with [nox](https://nox.thea.codes/en/stable/), which uses
pytest.

```sh
nox -s tests
```

Nox also has
