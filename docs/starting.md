# Starting Out

## Installation

### Requirements
Lasso uses Python 3.6 and above.  Requirements are stored in `requirements.txt` but are automatically installed when using `pip` as are development requirements (for now) which are located in `dev-requirements.txt`.

### Basic install instructions
If you are managing multiple python versions, we suggest using [`virtualenv`](https://virtualenv.pypa.io/en/latest/) or [`conda`](https://conda.io/en/latest/) virtual environments.

Example using a conda environment (recommended):

```bash
conda create python=3.7 -n <my_lasso_environment>
source activate <my_lasso_environment>
conda install rtree
conda install shapely
conda install fiona
pip install network_wrangler
pip install git+https://github.com/wsp-sag/Lasso@master#egg=lasso

```

Note: if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`

### Recommended: Installing from a Git Clone
If you are going to be working on Lasso locally or want to update your
installation frequently from GitHub, you might want to clone it to your
local machine and install it from the clone in [editable mode](https://pip.pypa.io/en/stable/reference/pip_install/?highlight=editable#editable-installs) .  

**if you plan to do development on both network wrangler and lasso locally, consider installing network wrangler from a clone as well!**

```bash
conda create python=3.7 -n <my_lasso_environment>
source activate <my_lasso_environment>
conda install rtree
conda install shapely
conda install fiona

pip install network_wrangler

git clone https://github.com/wsp-sag/Lasso
cd lasso
pip install -e .
```

Notes:

1. The -e installs it in editable mode.
2. If you are not part of the project team and want to contribute code bxack to the project, please fork before you clone and then add the original repository to your upstream origin list per [these directions on github](https://help.github.com/en/articles/fork-a-repo).
3. if you wanted to install from a specific tag/version number or branch, replace `@master` with `@<branchname>`  or `@tag`
4. If you want to make use of frequent developer updates for network wrangler as well, you can also install it from clone by copying the instructions for cloning and installing Lasso for Network Wrangler

If you are going to be doing Lasso development, we also recommend:
 -  a good IDE such as [Atom](http://atom.io), VS Code, Sublime Text, etc.
 with Python syntax highlighting turned on.  
 - [GitHub Desktop](https://desktop.github.com/) to locally update your clones   

## Brief Intro

Lasso is a 'wrapper' around the [Network Wrangler](http://wsp-sag.github.io/network_wrangler) utility.  

Both Lasso and NetworkWrangler are built around the following data schemas:
 - [roadway network], which is based on a mashup of Open Street Map and [Shared Streets](http://sharedstreets.io).  In Network Wrangler these are read in from three json files reprsenting: links, shapes, and nodes.
 - [transit network], which is based on a frequency-based implementation of the csv-based GTFS; and
 - [project cards], which is novel to Network Wrangler and stores information about network changes as a result of projects in yml.

In addition, Lasso utilized the following data schemas:

 - [MetCouncil Model Roadway Network Schema];  and  å
 - [MetCouncil Model Transit Network Schema], which uses the Cube PublicTransport format,  
 - [Cube Log Files], which document changes to the roadway network.  


### Components
Network Wrangler has the following atomic parts:

 - _RoadwayNetwork_ object, which represents the roadway network data as GeoDataFrames;  
 - _TransitNetwork_ object, which represents the transit network data as DataFrames;  
 - _ProjectCard_ object, which represents the data of the project cards;  
 - _Scenario_ object, which consist of at least a RoadwayNetwork, and
TransitNetwork.  Scenarios can be based on or tiered from other scenarios.
Scenarios can query and add ProjectCards to describe a set of changes that should be made to the network.


In addition, Lasso has the following atomic parts:  
 - Project  
 - ModelRoadwayNetwork  
 - StandardTransit  
 - CubeTransit  
 - Parameters

#### RoadwayNetwork

#### TransitNetwork

#### ProjectCard

#### Scenario

#### Project
Creates project cards by comparing two MetCouncil Model Transit Network files or by reading a cube log file and a base network;  

```python

test_project = Project.create_project(
  base_transit_source=os.path.join(CUBE_DIR, "transit.LIN"),
  build_transit_source=os.path.join(CUBE_DIR, "transit_route_shape_change"),
  )

test_project.evaluate_changes()

test_project.write_project_card(
  os.path.join(SCRATCH_DIR, "t_transit_shape_test.yml")
  )

```

#### ModelRoadwayNetwork
A subclass of network_wrangler's RoadwayNetwork
class which additional understanding about how to translate and write the
network out to the MetCouncil Roadway Network schema.

```Python

net = ModelRoadwayNetwork.read(
      link_file=STPAUL_LINK_FILE,
      node_file=STPAUL_NODE_FILE,
      shape_file=STPAUL_SHAPE_FILE,
      fast=True,
  )

net.write_roadway_as_fixedwidth()

```

#### StandardTransit
Translates the standard GTFS data to MetCouncil's Cube Line files.

```Python
cube_transit_net = StandardTransit.read_gtfs(BASE_TRANSIT_DIR)
cube_transit_net.write_as_cube_lin(os.path.join(WRITE_DIR, "outfile.lin"))
```

_CubeTransit_, which is used by the project class and has the capability to:
    - Parse cube line file properties and shapes into python dictionaries  
    - Compare line files and represent changes as Project Card dictionaries  

```python
tn = CubeTransit.create_from_cube(CUBE_DIR)
transit_change_list = tn.evaluate_differences(base_transit_network)
```

#### Parameters
Holds information about default parameters but can
also be initialized to override those parameters at object instantiation using a dictionary.

```Python
# read parameters from a yaml configuration  file
# could also provide as a key/value pair
with open(config_file) as f:
      my_config = yaml.safe_load(f)

# provide parameters at instantiation of ModelRoadwayNetwork
model_road_net = ModelRoadwayNetwork.from_RoadwayNetwork(
            my_scenario.road_net, parameters=my_config.get("my_parameters", {})
        )
# network written with direction from the parameters given
model_road_net.write_roadway_as_shp()

```

### Typical Workflow

Workflows in Lasso and Network Wrangler typically accomplish one of two goals:  
1. Create Project Cards to document network changes as a result of either transit or roadway projects.
2. Create Model Network Files for a scenario as represented by a series of Project Cards layered on top of a base network.

#### Project Cards from Transit LIN Files


#### Project Cards from Cube LOG Files


#### Model Network Files for a Scenario



## Running Quickstart Jupyter Notebooks

To learn basic lasso functionality, please refer to the following jupyter notebooks in the `/notebooks` directory:  

 - `Lasso Project Card Creation Quickstart.ipynb`   
 - `Lasso Scenario Creation Quickstart.ipynb`  

 Jupyter notebooks can be started by activating the lasso conda environment and typing `jupyter notebook`:

 ```bash
 conda activate <my_lasso_environment>
 jupyter notebook
 ```
