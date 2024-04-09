"""Command line tool for working with Petrel input and output formats."""

from __future__ import annotations

import sys

if __name__ == "__main__":
    from petrelpy.cli import cli

    sys.exit(cli())
