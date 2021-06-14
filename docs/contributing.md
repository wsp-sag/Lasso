# Contributing

## Setting up environment

### Installation
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).


```bash
conda config --add channels conda-forge
conda create python=3.7 rtree geopandas osmnx -n <my_lasso_environment>
conda activate <my_lasso_environment>
git clone https://github.com/wsp-sag/Lasso
git clone https://github.com/wsp-sag/network_wrangler
cd network_wrangler
pip install -e .
cd ..
cd Lasso
pip install -e .
```

Notes:

1. The -e installs it in editable mode.
2. If you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).
3. if you wanted to install from a specific tag/version number or branch, replace `@main` with `@<branchname>`  or `@tag`
4. If you want to make use of frequent developer updates for network wrangler as well, you can also install it from clone by copying the instructions for cloning and installing Lasso for Network Wrangler

### Other tools
If you are going to be doing development, we also recommend a few other tools:
 -  a good IDE such as [VS Code](https://code.visualstudio.com/), Sublime Text, etc.
 with Python syntax highlighting turned on.
 - [GitHub Desktop](https://desktop.github.com/) to locally update your clones

## Development Workflow

General steps:
1. Assign yourself to an issue (https://github.com/wsp-sag/Lasso/issues). If an issue is not already created which addresses what you want to accomplish, please add it and discuss with Lasso community – especially w.r.t. which branch to work from (usually `develop`).
2. Fork the repository (https://github.com/wsp-sag/Lasso/fork)
3. Create your feature branch (`git checkout -b feature/fooBar`)
4. Add your feature, add tests which test your feature, document your feature, and pass all tests.
5. Commit your changes (`git commit -am '[feat] Add some fooBar'`) using a [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) compliant message
6. Push to the branch (`git push origin feature/fooBar`)
7. Create a new Pull Request

## Tests
Tests are written for `pytest` and can be run using the command `pytest`.

Tests are run with the [pyTest](pytest.org)/

### Test structure


### Setup

Pytest can be installed using one of the following options.

Install along with all development requirements (recommended):
```sh
pip install -r dev-requirements.txt
```
Install using PIP:
```sh
pip install pytest
```
Install using Conda:
```sh
conda install pytest
```

### Running tests

1. Run all tests
```sh
pytest
```

2. Run tests in `test_basic.py`
```sh
pytest tests/test_basic.py
```

3. Run tests decorated with @pytest.mark.favorites decorator
```sh
pytest -m favorites
```

4. Run all tests and print out stdout
```sh
pytest -s
```

## Continuous Integration

Lasso uses Travis CI to run tests for each push to the repository as configured in `travis.yml'

## Documentation
Docstrings are written in [google style](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html) and auto-rendered as api documentation using Sphinx AutoDoc as specified in `/docs/autodoc.rst`.

Documentation uses [SPHINX](https://www.sphinx.org/) and is in the `/docs` folder.  It can be rendered locally using the command `make html`.
