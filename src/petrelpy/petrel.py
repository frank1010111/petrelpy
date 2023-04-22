from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_header(df, fname, fill_na=-999):
    """Write header information to a Petrel-readable header file.

    Parameters
    ----------
    df : DataFrame
        header information for wells (does not pass index)
    fname : str
        file to write to
    fill_na : int, optional
        value to write null values to, by default -999
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


def read_header(fname):
    return read_petrel_tops(fname)


def collect_perfs(df_perf: pd.DataFrame) -> pd.DataFrame:
    """Group perforations by well

    Parameters
    ----------
    df_perf : pd.DataFrame
        Well completion data. Expected columns include "Date Completion",
        "Date First Report", "Depth Top", "Depth Base" and "UWI"

    Returns
    -------
    pd.DataFrame
        Perforations grouped by well
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


def export_perfs(out_df: pd.DataFrame, out_fname: str, header=None):
    if not header:
        header = """UNITS FIELD\n"""
    with Path(out_fname).open("w") as f:
        f.write(header)
        for api, well in out_df:
            f.write(f"\nWELLNAME {api}\n")
            for _, vals in well.iterrows():
                f.write(
                    f"{vals['Date']} perforation {vals['Depth Top']} {vals['Depth Base']} 1 0\n"
                )


def read_production(infile, yearly=False):
    "Get raw data from infile (even if infile is several files)"
    # read in data and group by well
    if yearly:
        sheetname = "Annual Production"
        cols = ["Annual Liquid", "Annual Water", "Annual Gas"]
    else:
        sheetname = "Monthly Production"
        cols = ["Liquid", "Water", "Gas"]

    def readfile(fname):
        if Path(fname).suffix == ".csv":
            Raw = pd.read_csv(fname, converters={"Year": str, "API": str})
        else:
            Raw = pd.read_excel(
                fname, sheet_name=sheetname, converters={"Year": str, "API": str}
            )
        return Raw

    if isinstance(infile, (list, tuple)):
        sheets = [readfile(fname) for fname in infile]
        raw_df = pd.concat(sheets)
    else:
        raw_df = readfile(infile)

    if yearly:
        raw_df["Date"] = pd.to_datetime(raw_df.Year, format="%Y")
    else:
        raw_df["Date"] = pd.to_datetime(raw_df.Month + raw_df.Year, format="%b%Y")

    # print(len(Raw),'reports found')

    wells = (
        raw_df.groupby(["API", "Date"])
        .agg({col: sum for col in cols})
        .reset_index()
        # .groupby('API')
    )
    return wells


def export_vol(wells, outfile, header=None):
    if not header:
        header = """
*Field
*MONTHLY
*DAY *MONTH *YEAR *OIL *WATER *GAS
"""

    # export data
    with Path(outfile).open("w") as f:
        f.write(header)
        for UWI, production in wells.groupby("API"):
            f.write(f"\n*NAME {UWI}\n")
            for _, vals in production.sort_values("Date").fillna(0).iterrows():
                f.write(
                    f"01 {vals.Date.month:02} {vals.Date.year}   "
                    f"{vals.Liquid:<6.2f} {vals.Water:<6.2f} {vals.Gas:<6.2f}\n"
                )
    return


def export_injection_vol(wells, outfile, header=None):
    if not header:
        header = """
*Field
*MONTHLY
*DAY *MONTH *YEAR *WATER *GAS
"""
    with Path(outfile).open("w") as f:
        f.write(header)
        for UWI, production in wells.groupby("API"):
            f.write(f"\n*NAME {UWI}\n")
            for _, vals in production.sort_values("Date").fillna(0).iterrows():
                f.write(
                    f"1 {vals.Date.month:<2d} {vals.Date.year}   "
                    f"{vals.Water:<6.0f} {vals.Gas:<6.0f}\n"
                )
    return


def convert_properties_petrel_to_arc(fin, fout, prop):
    df = pd.read_csv(
        fin,
        sep=" ",
        header=8,
        index_col=False,
        names=["i", "j", "k", "x_coord", "y_coord", "z_coord", prop],
    )
    layernum = df.k.unique()
    if len(layernum) == 10:
        df["layer"] = df.k.replace(
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
        df["layer"] = df.k.replace(
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

    dfo = df.set_index(["x_coord", "y_coord", "layer"])[[prop]].unstack("layer")
    dfo.columns = ["_".join(c) for c in dfo.columns.values]
    dfo.to_csv(fout)


def read_petrel_tops(fname: str) -> pd.DataFrame:
    """Read Petrel tops file.

    Parameters
    ----------
    fname : str
        path to file

    Returns
    -------
    pd.DataFrame
        Tops, with Well indicating the well, then a column for each surface
    """
    with Path(fname).open() as f:
        i = 0
        cols_started = False
        colnames = []
        for line in f:
            line = line.rstrip("\r\n")
            if line == "END HEADER":
                break
            i += 1
            if cols_started:
                colnames.append(line)
            if line == "BEGIN HEADER":
                cols_started = True
    try:
        df = pd.read_csv(
            fname,
            skiprows=i + 1,
            names=colnames,
            header=None,
            sep="\\s+",
            na_values=-999,
            dtype={"Well": str},
        )
    except UnicodeDecodeError:
        df = pd.read_csv(
            fname,
            skiprows=i + 1,
            names=colnames,
            header=None,
            sep="\\s+",
            na_values=-999,
            dtype={"Well": str},
            encoding="ISO-8859-1",
        )
    return df


def write_tops(df, fname, comments="", fill_na=-999):
    """Write top picks to Petrel-readable file.

    Parameters
    ----------
    df : DataFrame
        tops picks. Expects "Well" to be first column, then a column per horizon
    fname : str
        Path to write out
    comments : str, optional
        Any comments to include at the beginning of the file, by default ""
    fill_na : int, optional
        Value to assign nulls to, by default -999
    """
    header = "BEGIN HEADER\n" + "\n".join(df.columns) + "\nEND HEADER\n"
    body = df.fillna(fill_na).to_csv(
        header=False, index=False, quoting=2, sep=" ", line_terminator="\n"
    )
    with Path(fname).open("w") as f:
        f.write(comments + "\nVERSION 2\n" + header + body)
