"""Work with Eclipse well connection files.

These are a handy export from Petrel that can get you well-specific properties.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Iterator

import pandas as pd

COL_NAMES_TRAJECTORY = [
    "MD_ENTRY",
    "GRID_I",
    "GRID_J",
    "GRID_K",
    "WELL_ENTRY_X",
    "WELL_ENTRY_Y",
    "WELL_ENTRY_Z",
    "ENTRY_FACE",
    "MD_EXIT",
    "WELL_EXIT_X",
    "WELL_EXIT_Y",
    "WELL_EXIT_Z",
    "EXIT_FACE",
]


def process_well_connection_file(
    well_connection_file: str,
    wellname_to_heel: pd.DataFrame,
    property_aggregates: dict[str, Any],
    col_names: list,
) -> pd.DataFrame:
    """Get average properties along the laterals for a well connection file.

    Args:
        well_connection_file (str): Eclipse well connection file exported from Petrel
        wellname_to_heel (pd.DataFrame): DataFrame containing UWI,Name,Depth_Heel for
            each well in the connection file
        property_aggregates (dict[str,Any]): properties from the well connection file to
            extract and how to aggregate them
        col_names (list): dictionary of column names in the well connection file
            you can check this with get_tragectory_columns(well_connection_file)
            the first few are usually ['MD_ENTRY', 'GRID_I', 'GRID_J', 'GRID_K','WELL_ENTRY_X',
            'WELL_ENTRY_Y','WELL_ENTRY_Z','ENTRY_FACE','MD_EXIT','WELL_EXIT_X','WELL_EXIT_Y',
            'WELL_EXIT_Z','EXIT_FACE',]

    Output: pd.DataFrame
        DataFrame indexed by UWI, with columns that are the keys of property_aggregates

    Note: vertical wells get everything included, wells not in wellname_to_heel are treated
        as vertical and have their wellname as their index
    """
    with Path(well_connection_file).open() as f:
        property_frame = pd.DataFrame(
            [
                process_well_lateral(
                    ws, wellname_to_heel, property_aggregates, col_names
                )
                for ws in get_well(f)
            ]
        )
    return property_frame


def get_wellnames(wc_file):
    """Get all the well names from a well connection file."""
    with Path(wc_file).open() as f:
        wellnames = [
            row.replace("WELLNAME", "").strip()
            for row in f
            if row.startswith("WELLNAME")
        ]
    return wellnames


def get_trajectory_columns(fname: str | Path):
    """Get columns used for well trajectory."""
    with Path(fname).open() as f:
        trajectory = False
        columns = ""
        for row in f:
            # print(row)
            if trajectory:
                columns = row.strip()
                break
            if row.strip() == "TRAJECTORY_COLUMN_ORDER":
                trajectory = True
    return columns


def process_well_lateral(
    well_string: str,
    wellname_to_heel: pd.DataFrame,
    property_aggregates: dict,
    col_names: list,
) -> pd.Series:
    """Get average properties along the well lateral from the geomodel.

    Args:
        well_string (str): portion of well connection file containing one well
        wellname_to_heel (pd.DataFrame): DataFrame containing UWI,Name,Depth_Heel for each well
            in the connection file
        property_aggregates (dict): mapping from properties to aggregation methods
        col_names (list): columns for the well trajectory

    Returns:
        pd.Series: average properties along the well's lateral
    """
    wellname = get_wellname(well_string)
    trajectory = get_trajectory(well_string, col_names)
    try:
        uwi_heel = wellname_to_heel[wellname_to_heel["Name"] == wellname].iloc[0]
    except IndexError:
        uwi_heel = pd.Series({"UWI": wellname, "Depth_heel": 0})
    if trajectory.MD_ENTRY.max() < uwi_heel["Depth_heel"]:
        lateral = trajectory.iloc[[-1]]
    else:
        lateral = trajectory[uwi_heel["Depth_heel"] <= trajectory.MD_ENTRY]
    properties = lateral.agg(property_aggregates)
    if properties.shape[0] == 0:
        properties = properties.reindex([0])
    properties = properties.iloc[0].rename(uwi_heel["UWI"])
    return properties


def get_trajectory(well_string: str, col_names: list[str]) -> pd.DataFrame:
    """Get trajectory for a well from the well connection file.

    Args:
        well_string (str): well connection file portion
        col_names (list[str]): columns for the trajectory

    Returns:
        pd.DataFrame: properties along the trajectory
    """
    trajectory = well_string.split("TRAJECTORY")[1]
    if "END_" in trajectory:
        trajectory = trajectory.split("END_")[0]
    out_frame = pd.read_csv(
        io.StringIO(trajectory), sep="\\s+", names=col_names, na_values=["-999"]
    )
    return out_frame


def get_well(file_obj: Iterator[str]):
    """Get section of well connection file for well.

    Args:
        file_obj Iterator[str]: loaded well connection file, iterated over row-by-row

    Yields
        Iterator[str]: string containing well properties
    """
    in_well = False
    strings = ""
    for row in file_obj:
        if row.startswith("WELLNAME"):
            in_well = True
            strings += row
        elif row.startswith("END_TRAJECTORY") and in_well:
            in_well = False
            strings += row
            out_string = strings
            strings = ""
            yield out_string
        elif in_well:
            strings += row


def get_wellname(well_string: str) -> str:
    """Get well's name from string.

    Args:
        well_string (str): portion of well connection file with the well

    Returns:
        str: well's name
    """
    wellname = well_string.split("\n", 2)[0].replace("WELLNAME", "").strip()
    return wellname
