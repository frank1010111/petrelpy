# Getting started

## Installation

From the project's root directory, install the CLI tool using
[pipx](https://pypa.github.io/pipx/)

```bash
pipx install .
```

or, from anywhere,

```bash
pipx install git+https://github.com/frank1010111/petrelpy.git
```

This will put `petrelpy` on your path.

## Overview

The CLI tool gives you the new command `petrelpy`:

```bash
$ petrelpy --help
Usage: petrelpy [OPTIONS] COMMAND [ARGS]...

  Command line tool for working with Petrel input and output formats.

Options:
  -h, --help  Show this message and exit.

Commands:
  connection   Process well connection file to average geomodel properties.
  perforation  Create petrel perforation file.
  production   Convert IHS production spreadsheet to Petrel vol format.
```

Then, there are a few sub-commands.

::::::::{grid} 1

:::::::{grid-item} ::::::{dropdown} Exporting properties along the wellbore with
well `connection` files

This sub-command allows you to process Eclipse well connection files (`.wcf`) to
get average properties along the lateral for wells.

```bash
$ petrelpy connection -h
Usage: petrelpy connection [OPTIONS] INPUT

  Process well connection file to average geomodel properties.

  This gets well properties from Petrel (in an Eclipse format) into a
  spreadsheet.

Options:
  -o, --output PATH  well property aggregate file, defaults to input file
                     location with .csv extension
  -e, --heel PATH
  -h, --help         Show this message and exit.
```

The process is this:

1. Export a well connection file from Petrel
2. Make a heel spreadsheet with the columns UWI, Name, and Depth_Heel.

   - UWI is the 14-digit API number,
   - Name is the name Petrel has saved as the well name (it is used in the well
     connection file to denote each well)
   - Depth_Heel is the measured depth at which the lateral begins

3. Run the command like so...
   `petrelpy connection field.wcf -e heels.csv -o well_properties.csv`

:::::: :::::::

:::::::{grid-item} ::::::{dropdown} Importing `perforation` files into Petrel

This creates perforation files from spreadsheets to import into Petrel.

```bash
$ petrelpy perforation --help
Usage: petrelpy perforation [OPTIONS] [INPUT]...

  Create petrel perforation file.

  Takes csv, excel, or prn files as input. Expected columns include: API,
  Treatment Start Date, start_depth, stop_depth

  Produces .ev (default) or .prn file

Options:
  -o, --output PATH    petrel event file with perforations, defaults to first
                       input file with .ev extension
  -h, --header TEXT    header information to include in the event file
  -d, --date-col TEXT  column for perforation dates
  --sheetname INTEGER  sheet name for excel file inputs
  --help               Show this message and exit.
```

The process is this:

1. Download an IHS perforation spreadsheet. Let's say it's a csv... T
2. Adjust it such that the columns are "API", "Treatment Start Date",
   "start_depth", and "stop_depth"

   - API is the 14-digit unique well identifier
   - Treatment Start Date is the date the perforations were created
   - start_depth is the measured depth where the perforation starts
   - stop_depth is the measured depth where the perforation ends

3. Run the CLI tool like so... `petrelpy perforation perfs.csv -o perfs.ev`

:::::: :::::::

:::::::{grid-item} ::::::{dropdown} Importing `production` files into Petrel

This creates a vol file for production data that can be imported into Petrel.

```bash
$ petrelpy production --help
Usage: petrelpy production [OPTIONS] [INPUT]

  Convert IHS production spreadsheet to Petrel vol format.

  This allows production to be easily imported into Petrel.

Options:
  -o, --output PATH  Petrel vol file, defaults to input file location and name
                     with .vol extension
  -y, --yearly       whether to aggregate production by year, by default False
  -z, --zip          whether to unzip the file, by default False
  -h, --help         Show this message and exit.
```

The process is this:

1. Download an IHS-style production file. The important columns are API, Year,
   Month, Liquid, Water, Gas. If the yearly option is used, Month is not
   necessary and (Liquid, Water, Gas) add Annual like Annual Liquid.

   - API: The unique well identifier
   - Year: The production year
   - Month: The production month (sorry days)
   - Liquid: Oil production in bbl
   - Water: water production in bbl
   - Gas: gas production in Mcf

2. Run the CLI tool like...
   `petrelpy production production.csv -o production.vol`

:::::: :::::::

::::::::
