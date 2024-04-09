"""Extract GSLIB data."""

from __future__ import annotations

import petrelpy

fin = "OOIP facies dep.txt"
ooip_cells = petrelpy.gslib.load_from_petrel(fin, npartitions=30)
ooip_cells = ooip_cells.dropna(subset=["Mainzones"]).drop(
    ["i_index", "j_index", "k_index", "x_coord", "y_coord", "z_coord"], 1
)

zone_name = "Mainzones"
facies_name = "Facies_BEG_DiscreteIK100K"
properties = ["PHITSGSfacies-dep", "SWTSGSfacies-dep"]
ooip_name = "OOIP_Facies_dep"
aggregators = {
    "OOIP_Facies_dep": "sum",
    "Bulkvolume": "sum",
    "PHITSGSfacies-dep": "mean",
    "SWTSGSfacies-dep": "mean",
}

ooip = petrelpy.gslib.get_facies_stats(ooip_cells, zone_name, facies_name, aggregators)
ooip.to_csv("OOIP_summary.csv")
print("saved OOIP summary")  # noqa: T201

ooip_splits, conversion, prop_max = petrelpy.gslib.get_facies_histograms(
    ooip_cells, zone_name, facies_name, properties, ooip_name
)

print("Calculated OOIP splits")  # noqa: T201
ooip_splits.to_csv("OOIP_hist.csv")
conversion.to_csv("OOIP_hist_converter.csv")
prop_max.to_csv("property_maxes.csv")
