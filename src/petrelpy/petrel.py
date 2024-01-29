"""Convert between Petrel and various other formats."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_header(df, fname, fill_na=-999):
    """Write header information to a Petrel-readable header file.

    Args:
        df (DataFrame): header information for wells (does not pass index)
        fname (str): file to write to
        fill_na (int, optional): value to write null values to, by default -999
    """
    header_head = (
        """# Petrel well head
VERSION 1
BEGIN HEADER
"""
        + "\n".join(df.columns)
        + "\nEND HEADER\n"
    )
    body = df.fillna(fill_na).to_csv(
        header=False, index=False, quoting=2, sep=" ", line_terminator="\n"
    )
    with Path(fname).open("w") as f:
        f.write(header_head + body)


def read_header(fname: str) -> pd.DataFrame:
    """Read headers from a file."""
    return read_petrel_tops(fname)


def collect_perfs(df_perf: pd.DataFrame) -> pd.DataFrame:
    """Group perforations by well.

    Args:
        df_perf (pd.DataFrame):
            Well completion data. Expected columns include "Date Completion",
            "Date First Report", "Depth Top", "Depth Base" and "UWI"

    Returns:
        pd.DataFrame: Perforations grouped by well
    """
    # gather dates
    df_perf["Date"] = df_perf["Date Completion"]
    df_perf.loc[df_perf.Date.isna(), "Date"] = df_perf["Date First Report"]
    df_perf["Date"] = pd.to_datetime(df_perf["Date"])

    out_df = (
        df_perf[["Depth Top", "Depth Base", "Date"]]
        .dropna()
        .assign(Date=lambda x: x.Date.dt.strftime("%m.%d.%Y"))
        .groupby(level=[0])
    )
    return out_df


# def export_perfs(out_df: pd.DataFrame, out_fname: str, header=None):
#     if not header:
#         header = """UNITS FIELD\n"""
#     with Path(out_fname).open("w") as f:
#         f.write(header)
#         for api, well in out_df:
#             f.write(f"\nWELLNAME {api}\n")
#             for _, vals in well.iterrows():
#                 f.write(
#                     f"{vals['Date']} perforation {vals['Depth Top']} {vals['Depth Base']} 1 0\n"
#                 )


def export_perfs_ev(
    perfs: pd.DataFrame, output: Path, header: str = "UNITS FIELD\n"
) -> None:
    """Export perforations in ev (event file) format.

    Args:
        perfs (pd.DataFrame): contains perfs in columns for API,start_depth,stop_depth
        output (Path): prn file to write to
        header (str): first line for file, probably explaining units
    """
    wells = perfs.groupby(level=0)  # group by well
    with Path(output).open("w") as f:
        f.write(header)
        for api, well in wells:
            f.write(f"\nWELLNAME {api}\n")
            for _, vals in well.iterrows():
                f.write(
                    "{} perforation {} {} 1 0\n".format(
                        *vals[["Date", "start_depth", "stop_depth"]]
                    )
                )


def export_perfs_prn(perfs: pd.DataFrame, output: Path) -> None:
    """Export perforations in prn (fixed) format.

    Args:
        perfs (pd.DataFrame): contains perfs in columns for API,start_depth,stop_depth
        output (Path): prn file to write to
    """
    prn_frame = (
        perfs.reset_index()
        .rename(columns={"API": "UWI", "start_depth": "Top", "stop_depth": "Bottom"})[
            ["UWI", "Top", "Bottom"]
        ]
        .assign(Perf=1)
        .sort_values("UWI")
    )
    with output.open("w") as f:
        f.write("UWI             Top    Bottom  Perf\n")
        for uwi, w in prn_frame.groupby("UWI"):
            old_bottom_location = 0
            for _, x in w.iterrows():
                top_location, bottom_location = x[["Top", "Bottom"]]
                if top_location <= old_bottom_location:
                    top_location = old_bottom_location + 1
                    if bottom_location < top_location:
                        bottom_location = top_location
                old_bottom_location = bottom_location
                f.write(f"{uwi:<14}  {top_location:<6.0f} {bottom_location:<6.0f}  1\n")


def read_production(infile: str | tuple[str], yearly=False):
    """Get raw data from infile (even if infile is several files)."""
    # read in data and group by well
    if yearly:
        sheetname = "Annual Production"
        cols = ["Annual Liquid", "Annual Water", "Annual Gas"]
    else:
        sheetname = "Monthly Production"
        cols = ["Liquid", "Water", "Gas"]

    def readfile(fname):
        if Path(fname).suffix == ".csv":
            raw_df = pd.read_csv(fname, converters={"Year": str, "API": str})
        else:
            raw_df = pd.read_excel(
                fname, sheet_name=sheetname, converters={"Year": str, "API": str}
            )
        return raw_df

    if isinstance(infile, (list, tuple)):
        sheets = [readfile(fname) for fname in infile]
        raw_df = pd.concat(sheets)
    else:
        raw_df = readfile(infile)

    if yearly:
        raw_df["Date"] = pd.to_datetime(raw_df.Year, format="%Y")
    else:
        raw_df["Date"] = pd.to_datetime(raw_df.Month + raw_df.Year, format="%b%Y")

    wells = (
        raw_df.groupby(["API", "Date"]).agg({col: sum for col in cols}).reset_index()
    )
    return wells


def export_vol(wells: pd.DataFrame, outfile: str | Path, header: str | None = None):
    """Export production volumes to Petrel-readable .vol format file.

    Args:
        wells (pd.DataFrame): IHS-style wells dataframe with oil, water, and gas production.
            Expected columns are API,Date,Liquid,Water,Gas.
        outfile (str | Path): vol file to save to
        header (str | None, optional): Units and column names. Defaults to None.
    """
    if any(wells.columns.to_series().str.startswith("Annual")):
        yearly = True
        wells = wells.rename(columns=lambda col: col.replace("Annual ", ""))
    else:
        yearly = False
    if not header:
        header = (
            "\n"
            "*Field\n"
            f"*{'YEARLY' if yearly else 'MONTHLY'}\n"
            "*DAY *MONTH *YEAR *OIL *WATER *GAS\n"
        )

    # export data
    with Path(outfile).open("w") as f:
        f.write(header)
        for uwi, production in wells.groupby("API"):
            f.write(f"\n*NAME {uwi}\n")
            for _, vals in production.sort_values("Date").fillna(0).iterrows():
                f.write(
                    f"01 {vals.Date.month:02} {vals.Date.year}   "
                    f"{vals.Liquid:<6.2f} {vals.Water:<6.2f} {vals.Gas:<6.2f}\n"
                )
    return


def export_injection_vol(wells, outfile, header=None):
    """Export injection volumes to Petrel-readable .vol format file.

    Args:
        wells (pd.DataFrame): IHS-style wells dataframe with water, and gas injection.
            Expected columns are API,Date,Water,Gas.
        outfile (str | Path): vol file to save to
        header (str | None, optional): Units and column names. Defaults to None.
    """
    if not header:
        header = """
*Field
*MONTHLY
*DAY *MONTH *YEAR *WATER *GAS
"""
    with Path(outfile).open("w") as f:
        f.write(header)
        for uwi, production in wells.groupby("API"):
            f.write(f"\n*NAME {uwi}\n")
            for _, vals in production.sort_values("Date").fillna(0).iterrows():
                f.write(
                    f"1 {vals.Date.month:<2d} {vals.Date.year}   "
                    f"{vals.Water:<6.0f} {vals.Gas:<6.0f}\n"
                )
    return


def convert_properties_petrel_to_arc(fin, fout, prop):
    """Make Midland basin Petrel gslib file Arc-readable."""
    geomodel = pd.read_csv(
        fin,
        sep=" ",
        header=8,
        index_col=False,
        names=["i", "j", "k", "x_coord", "y_coord", "z_coord", prop],
    )
    layernum = geomodel.k.unique()
    if len(layernum) == 10:
        geomodel["layer"] = geomodel.k.replace(
            {
                1: "USB",
                2: "MSB",
                3: "LSB",
                4: "ML",
                5: "Dean",
                6: "WCA",
                7: "WCB",
                8: "WCC1",
                9: "WCC2",
                10: "WCD",
            }
        )
    elif len(layernum) == 12:
        geomodel["layer"] = geomodel.k.replace(
            {
                1: "above",
                2: "USB",
                3: "MSB",
                4: "LSB",
                5: "ML",
                6: "Dean",
                7: "WCA",
                8: "WCB",
                9: "WCC1",
                10: "WCC2",
                11: "WCD",
                12: "STR",
            }
        )
    else:
        errmsg = f"the number of k-layers is not 10 or 12, it's {layernum}"
        raise (ValueError(errmsg))

    arc_layers = geomodel.set_index(["x_coord", "y_coord", "layer"])[
        [prop]
    ].pivot_table(columns="layer")
    arc_layers.columns = ["_".join(c) for c in arc_layers.columns.to_numpy()]
    arc_layers.to_csv(fout)


def read_petrel_tops(fname: str) -> pd.DataFrame:
    """Read Petrel tops file.

    Args:
        fname (str): path to file

    Returns:
        pd.DataFrame: Tops, with Well indicating the well, then a column for each surface
    """
    with Path(fname).open() as f:
        i = 0
        cols_started = False
        colnames = []
        for line in f:
            clean_line = line.rstrip("\r\n")
            if clean_line == "END HEADER":
                break
            i += 1
            if cols_started:
                colnames.append(clean_line)
            if clean_line == "BEGIN HEADER":
                cols_started = True
    try:
        well_tops = pd.read_csv(
            fname,
            skiprows=i + 1,
            names=colnames,
            header=None,
            sep="\\s+",
            na_values=-999,
            dtype={"Well": str},
        )
    except UnicodeDecodeError:
        well_tops = pd.read_csv(
            fname,
            skiprows=i + 1,
            names=colnames,
            header=None,
            sep="\\s+",
            na_values=-999,
            dtype={"Well": str},
            encoding="ISO-8859-1",
        )
    return well_tops


def write_tops(df, fname, comments="", fill_na=-999):
    """Write top picks to Petrel-readable file.

    Args:
        df (DataFrame): tops picks. Expects "Well" to be first column, then a column per horizon
        fname (str): Path to write out
        comments (str, optional):  Any comments to include at the beginning of the file,
            by default ""
        fill_na (int, optional): Value to assign nulls to, by default -999
    """
    header = "BEGIN HEADER\n" + "\n".join(df.columns) + "\nEND HEADER\n"
    body = df.fillna(fill_na).to_csv(
        header=False, index=False, quoting=2, sep=" ", line_terminator="\n"
    )
    with Path(fname).open("w") as f:
        f.write(comments + "\nVERSION 2\n" + header + body)


def get_raw_table(fname: str | Path, sheetname: int | str = 0) -> pd.DataFrame:
    """Ingest excel, prn, or csv file.

    Args:
        fname (str): file to read
        sheetname (int | str, optional): sheet to extract if excel. Defaults to 0.

    Returns:
        pd.DataFrame: table
    """
    fname = Path(fname)
    if ".xls" in fname.suffix:
        raw_frame = pd.read_excel(fname, sheetname, index_col=0)
    elif fname.suffix == ".prn":
        raw_frame = pd.read_csv(fname, sep="\\s+", index_col=0)
    else:
        raw_frame = pd.read_csv(fname, index_col=0)
    return raw_frame
