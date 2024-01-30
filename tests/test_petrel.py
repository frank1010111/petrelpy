"""Test petrelpy functions."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner
from petrelpy.cli import cli
from petrelpy.wellconnection import (
    COL_NAMES_TRAJECTORY,
    get_trajectory_geomodel_columns,
    process_well_connection_file,
)


@pytest.mark.parametrize("yearly", [True, False])
def test_cli_production(yearly):
    runner = CliRunner()
    input_csv = (
        Path(__file__).parent
        / f"data/test_{'yearly' if yearly else 'monthly'}_prod.csv"
    )
    with runner.isolated_filesystem() as td:
        out_file = Path(td) / "test_monthly_prod.vol"
        args = ["production", f"{input_csv}", "-o", f"{out_file}"]
        if yearly:
            args += ["-y"]
            assert args[-1] == "-y"
        result = runner.invoke(cli, args)
        test_output = out_file.open().read()
    assert result.exit_code == 0
    with Path("tests/data/test_monthly_prod.vol").open() as f:
        output_vol = f.read()
        if yearly:
            assert test_output == output_vol.replace("*MONTHLY", "*YEARLY")
        else:
            assert test_output == output_vol


def test_cli_wellconnection():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        wcf_file = Path(__file__).parent / "data/test_wcf.wcf"
        well_heel_file = Path(__file__).parent / "data/test_heels.csv"
        out_file = Path(td) / "test_properties.csv"
        args = [
            "connection",
            f"{wcf_file}",
            "--heel",
            f"{well_heel_file}",
            "-o",
            f"{out_file}",
        ]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0
        bench = wcf_file.with_suffix(".csv")
        with out_file.open() as f_output, Path(bench).open() as f_benchmark:
            assert f_output.read() == f_benchmark.read()


@pytest.mark.parametrize("output_format", ["ev", "prn"])
@pytest.mark.parametrize("input_format", ["csv", "xlsx", "prn"])
def test_cli_perforations(input_format, output_format):
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        main_perf_file = Path(__file__).parent / "data/test_perf.csv"
        if input_format == "csv":
            perf_file = main_perf_file
        elif input_format == "xlsx":
            perf_file = Path(td) / "test_perf.xlsx"
            pd.read_csv(main_perf_file).to_excel(perf_file, index=False)
        elif input_format == "prn":
            perf_file = Path(td) / "test_perf.prn"
            pd.read_csv(main_perf_file).to_csv(perf_file, sep=" ", index=False)
        else:
            msg = "Something has gone wrong with your file extensions, should be csv,xlsx, or prn"
            raise ValueError(msg)
        output_file = Path(td) / f"petrel_perfs.{output_format}"
        args = ["perforation", f"{perf_file}", "-o", f"{output_file}"]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0
        if output_format == "ev":
            bench = main_perf_file.with_suffix(".ev")
        else:
            bench = main_perf_file.parent / "benchmark_perf.prn"
        with output_file.open() as f_output, bench.open() as f_benchmark:
            assert f_output.read() == f_benchmark.read()


@pytest.mark.parametrize("output_format", ["csv", "parquet"])
def test_cli_gslib(output_format):
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        gslib_file = Path(__file__).parent / "data/test_geomodel.gslib"
        output_file = Path(td) / "geomodel"
        args = [
            "gslib",
            f"{gslib_file}",
            "-o",
            f"{output_file}",
            "--output_format",
            f"{output_format}",
        ]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0
        if output_format == "csv":
            bench = gslib_file.with_suffix(".csv")
            with output_file.with_suffix(
                ".csv"
            ).open() as f_output, bench.open() as f_benchmark:
                assert f_output.read() == f_benchmark.read()
        else:  # output_format == "parquet"
            benchmark = pd.read_csv(gslib_file.with_suffix(".csv")).sort_values(
                ["i_index", "j_index"]
            )
            result_frame = pd.read_parquet(
                output_file.with_suffix(".parquet")
            ).sort_values(["i_index", "j_index"])
            assert set(benchmark.columns) == set(result_frame.columns)
            for col in benchmark.columns:
                assert pytest.approx(benchmark[col]) == result_frame[col]


def test_double_trajectory_columns():
    input = Path(__file__).parent / "data/test_duplicated.wcf"
    heel = Path(__file__).parent / "data/test_heels.csv"
    geomodel_cols = get_trajectory_geomodel_columns(input)
    all_cols = COL_NAMES_TRAJECTORY + geomodel_cols
    heel_frame = pd.read_csv(heel)
    aggregates = (
        process_well_connection_file(input, heel_frame, col_names=all_cols)
        .dropna(subset=["GRID_I"])
        .rename_axis(index="UWI")
    )
    assert "General_1" in aggregates.columns
