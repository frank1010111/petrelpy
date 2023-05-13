---
jupytext:
  formats: ipynb,md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.5
kernelspec:
  display_name: petrelpy
  language: python
  name: petrelpy
---

# 11 minutes to petrelpy

This is a short, tutorial-style introduction to `petrelpy`, using Petrel-esque
import and export files and some simple dataframe manipulation to show the uses
of the library for extracting data from Petrel export formats.

## Loading well connection data

A few useful Petrel exports are gslib (Full 3D property grids) and well
connection files. Let's start with a well connection file. Funnily enough,
thanks to how Eclipse feels (or doesn't feel) about unique well identifiers, you
first need to extract the API number and well name from the Petrel well
worksheet - and the measured depth of the heel, while you're at it.

```{code-cell} ipython3
import pandas as pd
from petrelpy.wellconnection import (
    process_well_connection_file,
    get_trajectory_columns,
    COL_NAMES_TRAJECTORY,
)

# wcf_file = "https://raw.githubusercontent.com/frank1010111/petrelpy/master/tests/data/test_wcf.wcf"
# heel_file = "https://raw.githubusercontent.com/frank1010111/petrelpy/master/tests/data/test_heels.csv"
wcf_file = "../../tests/data/test_wcf.wcf"
heel_file = "../../tests/data/test_heels.csv"

well_heels = pd.read_csv(heel_file, index_col=0)
well_heels
```

Now that we have that, let's get to those well connection files

```{code-cell} ipython3
columns = get_trajectory_columns(wcf_file)
geomodel_cols = columns.split("  ")[1:]
all_cols = COL_NAMES_TRAJECTORY + geomodel_cols
well_properties = (
    process_well_connection_file(wcf_file, well_heels, col_names=all_cols)
    .dropna(subset="GRID_I")
    .drop(columns=["MD_ENTRY", "GRID_I", "GRID_J", "GRID_K"])
)
well_properties
```

## Processing well data

And now you have a pandas dataframe with all the power therein. You could, umm,
get the hydrocarbon-filled porosity for the wells. And then get average total
porosity and HCFP for each dominant facies.

```{code-cell} ipython3
(
    well_properties.assign(
        **{
            "hydrocarbon-filled porosity": lambda props: props["Porosity - total"]
            * (1 - props["Water saturation"])
        }
    )
    .groupby("Facies")[["Porosity - total", "hydrocarbon-filled porosity"]]
    .mean()
)
```

Okay, that was more like 5 minutes. I guess that means we should do something
else!

## Loading gslib data

GSLIB is a tabular format for holding geomodel property extracts. Since these
tend to be millions or billions of cells, the extracts are loaded into
[dask dataframes](https://docs.dask.org/en/stable/dataframe.html) initially.

```{code-cell} ipython3
from petrelpy.gslib import load_from_petrel

gslib_file = "../../tests/data/test_geomodel.gslib"
geomodel_properties = load_from_petrel(gslib_file)
geomodel_properties
```

The API for dask is pretty close to pandas, except it's lazy, and at the end you
call the `compute` method to make it into a pandas dataframe. Let's calculate
the average porosity for each $j$ index.

```{code-cell} ipython3
geomodel_properties.groupby("j_index")["Porosity"].mean().compute()
```
