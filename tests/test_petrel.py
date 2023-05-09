"""Test petrelpy functions."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from petrelpy.cli import cli


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
    output_vol = Path("tests/data/test_monthly_prod.vol").open().read()
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
        with Path(out_file).open() as f_output, Path(bench).open() as f_benchmark:
            assert f_output.read() == f_benchmark.read()
