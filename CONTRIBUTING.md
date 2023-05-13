# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

## Types of Contributions

### Report Bugs

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

Or add a new CLI sub-command at `src/petrelpy/cli.py`! Then add tests for your
new code. These can be run with [nox](https://nox.thea.codes/en/stable/), which
uses pytest.

### Write Documentation

You can never have enough documentation! Please feel free to contribute to any
part of the documentation, such as the official docs, docstrings, or even on the
web in blog posts, articles, and such.

### Submit Feedback

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions are
  welcome :)

## Get Started!

Ready to contribute? Here's how to set up `petrelpy` for local development.

1. Download a copy of `petrelpy` locally.

   ```console
   git clone https://github.com/frank1010111/petrelpy.git
   ```

2. Install `petrelpy` using `pip`:

   ```console
   pip -e .[dev]
   ```

3. Use `git` (or similar) to create a branch for local development and make your
   changes:

   ```console
   git checkout -b name-of-your-bugfix-or-feature
   ```

4. When you're done making changes, check that your changes conform to any code
   formatting requirements and pass any tests.

To do this, if you have pipx, run

```console
pipx run nox
```

else, run

```console
pip install nox
nox
```

5. Commit your changes and open a pull request.

Nox also has a few other useful sessions:

```
⚡ nox -l
Nox sessions for linting, docs, and testing.

Sessions defined in /home//petrelpy/noxfile.py:

* lint -> Run the linter.
* tests -> Run the unit and regular tests.
- docs -> Build the docs. Pass "serve" to serve.
- build -> Build an SDist and a wheel.

sessions marked with * are selected, sessions marked with - are skipped.
```

## Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include additional tests if appropriate.
2. If the pull request adds functionality, the docs should be updated.
3. The pull request should work for all currently supported operating systems
   and versions of Python.

## Code of Conduct

Please note that the `petrelpy` project is released with a Code of Conduct. By
contributing to this project you agree to abide by its terms.
