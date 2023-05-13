# petrelpy

Getting data in and out of Petrel

Included are a CLI and library for

- handling large GSLIB outputs
- using tops to get attributes in and out of Petrel
- getting Petrel outputs into Arc-readable spreadsheets
- turning header spreadsheets into Petrel-readable header files
- turnig production into .vol files for Petrel
- turning completion spreadsheets into .ev files
- processing Eclipse well connection files to get per-well properties

## Installation

The easiest way to use the command line interface is with
[pipx](https://pypa.github.io/pipx/):

```sh
pipx install .
```

or

```sh
pip install git+https://github.com/frank1010111/petrelpy.git
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

## Getting started

Check out [the docs](https://petrelpy.readthedocs.io). Since this isn't live
quite yet, you can choose to use `nox -s docs -- serve` to see them locally.

## Contributing

New Petrel-helpers are always appreciated. See
[the contribution guidelines](CONTRIBUTING.md)
