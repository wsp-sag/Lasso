# Lasso
This package of utilities is a wrapper around the [`network_wrangler`](http://github.com/wsp-sag/network_wrangler) package for MetCouncil.  **IMPORTANT** Lasso will automagically install `network_wrangler` for you, so

It aims to have the following functionality:

 1. parse Cube log files and base highway networks and create ProjectCards for Network Wrangler  
 2. parse two Cube transit line files and create ProjectCards for NetworkWrangler  
 3. refine Network Wrangler highway networks to contain specific variables and settings for Metropolitan Council and export them to a format that can be read in by  Citilab's Cube software.

## Installation

### Requirements
Lasso uses Python 3.6 and above.  Requirements are stored in `requirements.txt` but are automatically installed when using `pip` as are development requirements (for now) which are located in `dev-requirements.txt`.

The intallation instructions use the [`conda`](https://conda.io/en/latest/) virtual environment manager and some use the ['git'](https://git-scm.com/downloads) version control software.

### Basic instructions
If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

Example using a conda environment (recommended) and using the package manager [pip](https://pip.pypa.io/en/stable/) to install Lasso from the source on GitHub.

```bash
conda config --add channels conda-forge
conda create python=3.7 -n <my_lasso_environment> rtree geopandas
source activate <my_lasso_environment>
pip install git+https://github.com/wsp-sag/Lasso@master#egg=lasso
```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

#### From Clone
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

**if you plan to do development on both network wrangler and lasso locally, consider installing network wrangler from a clone as well!**

```bash
conda config --add channels conda-forge
conda create python=3.7 -n <my_lasso_environment> rtree geopandas
source activate <my_lasso_environment>
git clone https://github.com/wsp-sag/Lasso
git clone https://github.com/wsp-sag/network_wrangler
cd network_wrangler
pip install -e .
cd ..
cd Lasso
pip install -e .
```

Note: if you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).

## Documentation
Documentation is located at [https://wsp-sag.github.io/Lasso/](https://wsp-sag.github.io/Lasso/)

Edit the source of the documentation  in the `/docs` folder.

To build the documentation locally requires additional packages found in the `dev_requirements.txt` folder.  

To install these into your conda python environment execute the following from the command line in the Lasso folder:

```bash
source activate <my_lasso_environment>
pip install -r dev-requirements.txt
```

To build the documentation, navigate to the `/docs` folder and execute the command: `make html`

```bash
cd docs
make html
```

## Usage

To learn basic lasso functionality, please refer to the following jupyter notebooks in the `/notebooks` directory:  

 - `Lasso Project Card Creation Quickstart.ipynb`   
 - `Lasso Scenario Creation Quickstart.ipynb`  

Jupyter notebooks can be started by activating the lasso conda environment and typing `jupyter notebook`:

```bash
conda activate <my_lasso_environment>
jupyter notebook
```

A few other very basic API tips:

```python
import lasso
##TODO
```

## Client Contact and Relationship
Repository created in support of Met Council Network Rebuild project. Project lead on the client side is [Rachel Wiken](Rachel.Wiken@metc.state.mn.us). WSP team member responsible for this repository is [David Ory](david.ory@wsp.com).

## Attribution  
This project is built upon the ideas and concepts implemented in the [network wrangler project](https://github.com/sfcta/networkwrangler) by the [San Francisco County Transportation Authority](http://github.com/sfcta) and expanded upon by the [Metropolitan Transportation Commission](https://github.com/BayAreaMetro/NetworkWrangler).
