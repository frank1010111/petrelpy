"""Work with gslib geomodel format."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.dataframe as dd
import fastparquet
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree


def load_from_petrel(fin: Path | str, npartitions=60) -> dd.DataFrame:
    """Load GSLIB geomodel file.

    Args:
        fin (Path | str): gslib Petrel output
        npartitions (int, optional): number of partitions for dask dataframe. Defaults to 60.

    Returns:
        dd.DataFrame: lazy-evaluated dataframe with geomodel properties
    """
    numprops = pd.read_csv(fin, sep=" ", skiprows=1, nrows=1, header=None)[0][0]
    head = pd.read_csv(fin, sep=" ", skiprows=2, nrows=numprops, header=None)
    geomodel = dd.read_csv(
        fin,
        sep=" ",
        header=numprops + 1,
        na_values=-999,
        names=list(head[0]),
    )
    geomodel = geomodel.repartition(npartitions=npartitions)


def get_midpoint_cell_columns(geomodel: dd.DataFrame, dir_out: str):
    """Find cell columns where UWI-index exists in the geomodel.

    Args:
        geomodel (dd.DataFrame): geocellular model extracted from gslib file
        dir_out (str): folder to store the parquet file in

    Returns:
        pd.DataFrame: vertical column of cells around the well's midpoint
    """
    # ID midpoints
    df_midpoints = geomodel.dropna(subset=["UWI-index"]).compute()

    # get cells where i or j match the midpoint i and j indexes
    df_i_or_j = geomodel[
        (geomodel["i_index"].isin(df_midpoints["i_index"]))
        & geomodel["j_index"].isin(df_midpoints["j_index"])
    ]
    df_i_or_j.to_parquet(dir_out)

    # go through groups in the Parquet file as pandas dataframes,
    # grabbing cells that match i_index and j_index
    idx = pd.IndexSlice
    df_ij = []
    pf = fastparquet.ParquetFile(dir_out)
    n = 1
    for dfp in pf.iter_row_groups():
        df_ij.append(
            dfp.dropna(thresh=3)
            .set_index(["i_index", "j_index", "k_index"])
            .sort_index()
            .loc[idx[df_midpoints["i_index"], df_midpoints["j_index"], :],]
        )
        logging.info(n, "row groups down")
        n += 1
    df_ij = pd.concat(df_ij)
    return df_ij


def _limit_column_height(
    midpoints: pd.DataFrame, geomodel: dd.DataFrame, zdmax: float = 1000
):
    """Cut down on size of model to depths near the midpoint.

    This limits the geomodel cell midpoint vertical space to within zdmax of distance
    between wells (in df_midpoints) and geomodel cells (in df_ij).

    Args:
        midpoints (pd.DataFrame): well midpoint information,
            includes the columns i_index,j_index,z_coord
        geomodel (dd.DataFrame): geocellular model after being limited to vertical columns
            containing a midpoint
        zdmax (float, optional): maximum distance between midpoint and cell center.
            Defaults to 1000.

    Returns:
        pd.DataFrame: geomodel filtered to cells near well midpoints
    """
    df_out = []

    for _, (i, j, z) in midpoints[["i_index", "j_index", "z_coord"]].iterrows():
        ijmatch = geomodel.xs((i, j), level=(0, 1), drop_level=False)
        # ijmatch = df_ij.xs(i,level=0,drop_level=False).xs(j,level=1,drop_level=False)

        df_out.append(ijmatch[abs(ijmatch["z_coord"] - z) < zdmax])
        # break
    df_out = pd.concat(df_out)
    return df_out


def _get_header(fin: Path | str) -> list[str]:
    """Extract gslib header."""
    with Path(fin).open() as f:
        header_lines = []
        in_header = False
        for line in f:
            clean_line = line.strip()
            if clean_line == "BEGIN HEADER":
                in_header = True
            elif in_header:
                header_lines.append(clean_line)
            elif clean_line == "END HEADER":
                break
    return header_lines


def load_petrel_tops_file(tops_file: Path | str):
    """Load in petrel tops file and return pandas dataframe."""
    colnames = _get_header(tops_file)
    header_rows = 1
    with Path(tops_file).open() as f:
        for line in f:
            header_rows += 1
            if line.strip() == "END HEADER":
                break
    well_tops = pd.read_csv(
        tops_file, skiprows=header_rows, names=colnames, sep="\\s+", na_values=-999
    )
    return well_tops


def match_well_to_cell(
    midpoints: pd.DataFrame, geomodel: pd.DataFrame, distance_upper_bound: float = 2000
) -> pd.DataFrame:
    """Give each well in df_wells the i,j,k index of the nearest cell in df_cells.

    Args:
        midpoints (pd.DataFrame): well midpoint locations. Columns include Well,X,Y,Z
        geomodel (pd.DataFrame): geocellular model. Columns include x_coord,y_coord,z_coord
        distance_upper_bound (float, optional): max distance between well and center of cell.
            Defaults to 2000.

    Returns:
        pd.DataFrame: geomodel cells nearest to each well midpoint
    """
    # make tree in 3D for getting nearest neighbors
    tree_cells = cKDTree(geomodel[["x_coord", "y_coord", "z_coord"]])
    dist, locs = tree_cells.query(
        midpoints.loc[:, ["X", "Y", "Z"]],
        k=1,
        distance_upper_bound=distance_upper_bound,
        n_jobs=4,
    )
    logging.warning((~np.isfinite(dist)).sum(), "wells could not get matches")

    # make output dataframe
    well_cell = midpoints.loc[:, ["Well", "X", "Y", "Z"]]
    for i in ("i_index", "j_index", "k_index"):
        well_cell.loc[:, i] = np.nan

    # assign i,j,k for each well to nearest cell
    for label, _w in well_cell.iterrows():
        if np.isfinite(dist[label]):
            well_cell.loc[label, ["i_index", "j_index", "k_index"]] = geomodel.iloc[
                locs[label]
            ].name
    return well_cell


def match_ijz_petrel(
    geomodel: dd.DataFrame, uwi_to_index: pd.DataFrame, wdir: str, zdmax: float = 1000
):
    """Merge geomodel with wells to assign properties to wells.

    Args:
        geomodel (dask.DataFrame): dask dataframe of the geomodel, must include a column
            called 'UWI-index', ijk indices, and z coordinate
        uwi_to_index (pd.DataFrame): pandas dataframe with UWI-Index column and UWI column to do
            merging
        wdir (str): string pointing to the working directory for behind-the-scenes parquet file
            creation
        zdmax (float): maximum z variation from cells in ij column to point including a UWI-index
    Returns:
        pd.DataFrame: merged dataframe that has all your favorite attributes in an ij column with
        less than zdmax vertical separation from midpoint at UWI-index.
    """
    df_midpoints = geomodel.dropna(subset=["UWI-index"]).compute()
    df_ijmatched = get_midpoint_cell_columns(geomodel, wdir)
    df_ijmatched_zlim = _limit_column_height(df_midpoints, df_ijmatched, zdmax)
    uwi_to_index["UWI-Index"] = uwi_to_index["UWI-Index"].astype(int)
    merged = (
        df_ijmatched_zlim.rename(columns={"UWI-index": "UWI-Index"})
        .reset_index()
        .merge(uwi_to_index, on="UWI-Index", how="left")
    )
    merged = merged[~merged.index.duplicated(keep="first")]
    return merged


def aggregate_well_properties(
    geomodel_ijmatched: pd.DataFrame,
    well_midponts: pd.DataFrame | None = None,
    zdmax: float = 200,
    agg_arg="mean",
    uwi_col="UWI-Index",
) -> pd.DataFrame:
    """Find average geocellular properties about well midpoint.

    Args:
        geomodel_ijmatched (pd.DataFrame): geomodel after paring down to nearby cells
        well_midponts (pd.DataFrame | None, optional): geocells nearest each well midpoint.
            Defaults to None. If none, cuts geomodel_ijmatched down to those with a valid uwi_col

        zdmax (float, optional): Max vertical distance between well and geocell. Defaults to 200.
        agg_arg (str, optional): method for aggregating. Defaults to "mean".
        uwi_col (str, optional): geocellular column with well identifier. Defaults to "UWI-Index".

    Returns:
        pd.DataFrame: average properties for each well index
    """
    if well_midponts is None:
        well_midponts = geomodel_ijmatched.dropna(subset=uwi_col)
    df_out_index = geomodel_ijmatched[uwi_col].dropna().unique()
    df_out_columns = geomodel_ijmatched.iloc[0:1].agg(agg_arg).columns
    well_properties = pd.DataFrame(index=df_out_index, columns=df_out_columns)

    for _, (i, j, z, uwi) in well_midponts[
        ["i_index", "j_index", "z_coord", uwi_col]
    ].iterrows():
        ijmatch = geomodel_ijmatched.xs((i, j), level=(0, 1), drop_level=False)
        zmatch = ijmatch[abs(ijmatch["z_coord"] - z) < zdmax]
        well_properties.loc[uwi] = zmatch.agg(agg_arg).iloc[0]
    return well_properties


def get_facies_stats(df, zonename="Mainzones", faciesname="Facies", attrs=None):
    """Get aggregated statistics for different facies and zones."""
    df_out = df.groupby([zonename, faciesname])[list(attrs.keys())]
    df_out = df_out.agg(attrs).compute()
    return df_out


def get_facies_histograms(
    geomodel: dd.DataFrame,
    zone_name: str = "Mainzones",
    facies_name: str = "Facies",
    properties: list[str] | None = None,
    ooip_name: str = "OOIP",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Calculate histogram of original oil in place for geomodel by zone, facies, properties.

    Args:
        geomodel (dd.DataFrame): geocellular model with zone, facies, properties
        zone_name (str, optional): column naming the zone. Defaults to "Mainzones".
        facies_name (str, optional): column naming the facies. Defaults to "Facies".
        properties (list[str], optional): columns holding properties to compute histogram over.
            Defaults to ["Phi", "Sw"].

        ooip_name (str, optional): Property to calculate the histogram over. Defaults to "OOIP".

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: ooip_splits, index_conversion, and
            max property values
    """
    if properties is None:
        properties = ["Phi", "Sw"]
    zones = geomodel[zone_name].unique().compute()
    facies = geomodel[facies_name].unique().compute()

    ooip_splits = pd.DataFrame(
        columns=pd.MultiIndex.from_product(
            [zones.to_numpy(), facies.to_numpy(), properties],
            names=["Zone", "Facies", "Property"],
        )
    ).sort_index(level="columns")

    # percentage of max for each property done over zone, facies
    prop_max = geomodel[properties].max().compute()
    for z in zones:
        for face in facies:
            df_slice = geomodel[
                (geomodel[zone_name] == z) & (geomodel[facies_name] == face)
            ]
            for p in properties:
                df_slice[p + "_"] = (
                    (df_slice[p] // (prop_max[p] / 100.0)).dropna().astype(int)
                )
                split = df_slice.groupby(p + "_")[ooip_name].sum().compute()
                ooip_splits.loc[:, (z, face, p)] = split
                logging.info(f"finished zone {z}, facies {face}, property {p}")

    ooip_splits = ooip_splits.sort_index()
    # provide translation from index to x values for histogram
    index_conversion = pd.DataFrame(columns=properties, index=ooip_splits.index)
    for p in properties:
        index_conversion[p] = prop_max[p] * index_conversion.index / 100.0

    return ooip_splits, index_conversion, prop_max
