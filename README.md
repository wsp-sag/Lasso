# Lasso (or whatever we decide to call it)
This package of utilities is a wrapper around the [network_wrangler](http://github.com/wsp-sag/network_wrangler) package for MetCouncil.  It aims to have the following functionality:

 1. parse Cube log files and base highway networks and create ProjectCards for Network Wrangler  
 2. parse two Cube transit line files and create ProjectCards for NetworkWrangler  
 3. refine Network Wrangler highway networks to contain specific variables and settings for Metropolitan Council and export them to a format that can be read in by  Citilab's Cube software.

## Installation

### Requirements
Lasso uses Python 3.6 and above.  Requirements are stored in `requirements.txt` but are automatically installed when using `pip` as are development requirements (for now) which are located in `dev-requirements.txt`

### Basic instructions
If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

Example using a conda environment (recommended):

```bash
conda create python=3.7 -n <my_lasso_environment>
source activate <my_lasso_environment>
conda install rtree
conda install shapely
conda install fiona
pip install git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
pip install git+https://github.com/wsp-sag/Lasso@master#egg=lasso
```

#### From GitHub
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Lasso from the source on GitHub.

```bash
conda install rtree
conda install shapely
pip install -e git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
pip install -e git+https://github.com/wsp-sag/Lasso@master#egg=lasso
```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

#### From Clone
If you are going to be working on Lasso locally, you might want to clone it to your local machine and install it from the clone.  The -e will install it in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs).

**if you plan to do development on both network wrangler and lasso locally, consider installing network wrangler from a clone as well!**

```bash
git clone https://github.com/wsp-sag/Lasso
cd lasso
pip install -e git+https://github.com/wsp-sag/network_wrangler.git@master#egg=network_wrangler
pip install -e .
```

Note: if you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).

## Documentation
Not currently up and running, but when it is...

Documentation requires the sphinx package and can be built from the `/docs` folder using the command: `make html`

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
