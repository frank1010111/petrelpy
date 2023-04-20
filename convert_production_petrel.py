# convert_production_petrel.py
# A script for converting IHS production data into a format readable by
# Petrel in the .vol format
# Copyright 2018 Frank Male


# A .vol file is supposed to look like this:
"""
*FIELD
*DAILY
*DAY *MONTH *YEAR *OIL *WATER *GAS *WINJ

*NAME Producer_1
01 01 1970  108.80  0.15  162.27  0
01 01 1971  197.05  0.29  290.15  0
01 01 1972  250.57  0.39  363.80  0
01 01 1973  334.36  0.56  477.98  0
01 01 1974  227.94  0.40  323.04  0

*NAME Injector_1
01 01 1970  0  0  0  1000
01 01 1971  0  0  0  1120
"""
from pathlib import Path

from petrelpy.petrel import read_production, export_vol
import argparse


if __name__ == "__main__":
    # get arguments from command line
    parser = argparse.ArgumentParser(
        description="Convert production data from IHS format to Petrel format"
    )
    parser.add_argument(
        "infile", metavar="INFILE", nargs="+", help="IHS style input file"
    )
    parser.add_argument("-o", "--outfile", default="", help="Petrel style output file")
    parser.add_argument(
        "-y", "--yearly", action="store_true", help="Input and Output yearly production"
    )
    parser.add_argument("-z", "--zip", action="store_true", help="Unzip IHS file")
    args = parser.parse_args()

    infile = args.infile

    if not args.outfile:
        outfile = Path(infile).with_suffix(".vol")
    else:
        outfile = args.outfile

    if args.zip:
        from zipfile import ZipFile

        fin = args.infile[0]
        fzip = ZipFile(fin)
        inside_zip = Path(fin).with_suffix("_Monthly Production.csv")
        infile = fzip.open(inside_zip)

    wells = read_production(infile, yearly=args.yearly)
    export_vol(wells, outfile, header=None)
