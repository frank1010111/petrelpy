# convert_production_petrel.py
# A script for converting IHS completion data into a format readable by
# Petrel in the .ev format
# Copyright 2018 Frank Male

import pandas as pd
from pathlib import Path
import argparse


if __name__ == "__main__":
    # get arguments from command line
    parser = argparse.ArgumentParser(
        description="Convert perforation data from IHS format to Petrel-readable .ev format"
    )
    parser.add_argument(
        "infile", metavar="INFILE", help="IHS style input file", nargs="+"
    )
    parser.add_argument(
        "-o", "--outfile", default="", help="Petrel .ev style output file"
    )
    parser.add_argument(
        "--header", default="", help="file to read in for header information"
    )
    parser.add_argument(
        "--sheetname", default=0, help="sheet name for excel file inputs"
    )
    parser.add_argument(
        "--date_col",
        default="Treatment Start Date",
        help="column for perforation dates",
    )
    args = parser.parse_args()

    infile = args.infile
    sheetname = args.sheetname
    if not args.outfile:
        outfile = Path(infile).with_suffix(".ev")
    else:
        outfile = args.outfile
    if args.header:
        header = args.header
    else:
        header = """\
    UNITS FIELD
    """

    # read in data and fix column names
    if ".xls" in infile[0]:
        Raw = pd.concat([pd.read_excel(i, sheetname, index_col=0) for i in infile])
    elif ".prn" in i:
        Raw = pd.concat([pd.read_csv(i, sep="\s+", index_col=0) for i in infile])
    else:
        Raw = pd.concat([pd.read_csv(i, index_col=0) for i in infile])

    print(len(Raw), "reports found")

    Raw.rename(
        columns={
            "Pressure Instantaneous Shutin (PSIG)": "pressure_shutin",
            "Depth Top": "depth_top",
            "Depth Base": "depth_base",
        },
        inplace=True,
    )
    assert args.date_col in Raw.columns, f"Date column not found: {args.date_col}"

    Raw["Date"] = Raw[args.date_col].dt.strftime("%m.%d.%Y")

    Wells = Raw.groupby(level=0)

    # export data
    with open(outfile, "wb") as f:
        f.write(header)
        for API, well in Wells:
            f.write("\nWELLNAME {}\n".format(API))
            for _, vals in well.iterrows():
                f.write(
                    "{} perforation {} {} 1 0\n".format(
                        *vals[["Date", "start_depth", "stop_depth"]]
                    )
                )


# # export PRN
# PRN = (Raw.rename(columns={'API':'UWI','start_depth':'Top','stop_depth':'Bottom'})
#        [['UWI','Top','Bottom']]
#        .assign(Perf=1)
#        .sort_values('UWI')
# )
# n=0
# with open(outfile,'wb') as f:
#     f.write("UWI             Top    Bottom  Perf\n")
#     for uwi,w in PRN.groupby('UWI'):
#         oldb = 0
#         for i,x in w.iterrows():
#             t,b = x[['Top','Bottom']]
#             if t<= oldb:
#                 n+=1
#                 t=oldb+1
#                 if b<t:
#                     b=t
#             oldb = b
#             f.write("{}  {:<6.0f} {:<6.0f}  1\n".format(uwi,t,b))

# print n,'corrections'

# Wells = Raw.groupby('API',sort=False)

# # export data
# with open(outfile,'wb') as f:
#     f.write(header)
#     for API,well in Wells:
#         f.write("\nWELLNAME {}\n".format(API))
#         for _,vals in well.iterrows():
#             f.write("{} {} {} {} {} 0\n".format(*vals[['date','event','start_depth','stop_depth','efficiency']]))

# Raw.columns = ('API','start_depth','stop_depth','date','status')
