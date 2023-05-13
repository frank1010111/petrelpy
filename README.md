# petrelpy

Getting data in and out of Petrel

Included are a command line tool and library for importing

- production
- injection
- completions information
- well formation tops, and
- headers

into Petrel, and exporting

- per-well geomodel properties
- GSLIB cellular outputs, and
- geomodel properties at tops

## Installation

The easiest way to use the command line interface is with
[pipx](https://pypa.github.io/pipx/):

```sh
pipx install .
```

or

```sh
pipx install git+https://github.com/frank1010111/petrelpy.git
```

This will put `petrelpy` on your path, and you can invoke the CLI tool with

```
$ petrelpy --help
Usage: petrelpy [OPTIONS] COMMAND [ARGS]...

  Command line tool for working with Petrel input and output formats.

Options:
  -h, --help  Show this message and exit.

Commands:
  production  Convert IHS production spreadsheet to Petrel vol format.
```

In order to use the library, install with pip:

```sh
pip install git+https://github.com/frank1010111/petrelpy.git
```

## Getting started

Check out [the docs](https://petrelpy.readthedocs.io). If it isn't live quite
yet, you can choose to use `nox -s docs -- serve` to see them locally.

## Contributing

New Petrel-helpers are always appreciated. See
[the contribution guidelines](CONTRIBUTING.md)

## The Stormy Petrel

    A thousand miles from land are we,
    Tossing about on the roaring sea, -
    From billow to bounding billow cast,
    Like fleecy snow on the stormy blast.
    The sails are scattered abroad like weeds;
    The strong masts shake like quivering reeds;
    The mighty cables and iron chains,
    The hull, which all earthly strength disdains, -
    They strain and they crack; and hearts like stone
    Their natural, hard, proud strength disown.

    Up and down! - up and down!
    From the base of the wave to the billow’s crown,
    And amidst the flashing and feathery foam
    The stormy petrel finds a home, -
    A home, if such a place may be
    For her who lives on the wide, wide sea,
    On the craggy ice, in the frozen air,
    And only seeketh her rocky lair
    To warm her young, and to teach them spring
    At once o’er the waves on their stormy wing!

    O’er the deep! - o’er the deep!
    Where the whale and the shark and the sword-fish sleep, -
    Outflying the blast and the driving rain,
    The petrel telleth her tale — in vain;
    For the mariner curseth the warning bird
    Which bringeth him news of the storm unheard!
    Ah! thus does the prophet of good or ill
    Meet hate from the creatures he serveth still;
    Yet he ne’er falters, - so, petrel, spring
    Once more o’er the waves on thy stormy wing!

[Barry Cornwall](https://mypoeticside.com/show-classic-poem-6226)
