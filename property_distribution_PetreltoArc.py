"""Estimate average geomodel properties for Arc."""

from __future__ import annotations

import pandas as pd
from petrelpy.petrel import convert_properties_petrel_to_arc


def main():
    """Run geomodel property averages."""
    # Pressure
    convert_properties_petrel_to_arc(
        "/home/malef/Downloads/All TORA Pressure XYZ Gslib Midland Basin.txt",
        "/home/malef/West Texas data/Pressure.csv",
        "Press",
    )
    # API gravity
    convert_properties_petrel_to_arc(
        "/home/malef/Downloads/All TORA API gravity from Midpoints XYZ Gslib Midland Basin.txt",
        "/home/malef/West Texas data/Gravity.csv",
        "API",
    )
    # water saturation
    convert_properties_petrel_to_arc(
        "/home/malef/Downloads/All TORA Water Saturation SWT XYZ Gslib Midland Basin.txt",
        "/home/malef/West Texas data/Sw.csv",
        "SW",
    )

    # z-values
    api_gravity = pd.read_csv(
        "/home/malef/Downloads/All TORA API gravity from Midpoints XYZ Gslib Midland Basin.txt",
        sep=" ",
        header=8,
        index_col=False,
        names=["i", "j", "k", "x_coord", "y_coord", "Z", "API"],
    )
    api_gravity["layer"] = api_gravity.k.replace(
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
    z_locations = api_gravity.pivot_table("Z", ["x_coord", "y_coord"], "layer")
    # z_locations = api_gravity.set_index(["x_coord", "y_coord", "layer"])[["Z"]].unstack(
    #     "layer"
    # )
    z_locations.columns = ["_".join(c) for c in z_locations.columns.to_numpy()]
    z_locations.to_csv("/home/malef/West Texas data/Z.csv")


if __name__ == "__main__":
    # main()
    # #Isochore
    # propdist_robintoguin('../WestTexas_raw/All TORA Isochore XYZ Gslib Midland Basin.txt',
    #                      '../WestTexas_raw/Isochore.csv',
    #                      'D')

    # #PhiT
    # propdist_robintoguin('../WestTexas_raw/All TORA Porosity PHIT XYZ Gslib Midland Basin.txt',
    #                      '../WestTexas_raw/PhiT.csv',
    #                      'PhiT')

    # #Ro
    convert_properties_petrel_to_arc(
        "../WestTexas_raw/All TORA Vitrinite Reflection Ro XYZ Gslib Midland Basin.txt",
        "../WestTexas_raw/Vitrinite_reflectance.csv",
        "Ro",
    )
