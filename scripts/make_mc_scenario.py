import os
import pandas as pd
from network_wrangler import RoadwayNetwork
from network_wrangler import TransitNetwork
from network_wrangler import ProjectCard
from network_wrangler import Scenario
from network_wrangler import WranglerLogger
from lasso import ModelRoadwayNetwork
import warnings

warnings.filterwarnings("ignore")
import yaml
import sys
import ast

USAGE = """
  python make_mc_scenario.py  mc_config.py
  """

if __name__ == "__main__":

    args = sys.argv

    if len(args) == 1:
        raise ValueError("ERROR - config file must be passed as an argument!!!")

    config_file = args[1]

    if not os.path.exists(config_file):
        raise FileNotFoundError(
            "Specified config file does not exists - {}".format(config_file)
        )

    WranglerLogger.info("\nReading config file: {}".format(config_file))
    with open(config_file) as f:
        my_config = yaml.safe_load(f)

    # Create Base Network
    WranglerLogger.info("\nCreating base scenario")
    base_scenario = Scenario.create_base_scenario(
        my_config["base_scenario"]["shape_file_name"],
        my_config["base_scenario"]["link_file_name"],
        my_config["base_scenario"]["node_file_name"],
        base_dir=my_config["base_scenario"]["input_dir"],
    )

    # Create Scenaro Network
    if len(my_config["scenario"]["project_cards_filenames"]) > 0:
        WranglerLogger.info("\nCreating project card objects for scenario")
    project_cards_list = [
        ProjectCard.read(filename, validate=False)
        for filename in my_config["scenario"]["project_cards_filenames"]
    ]

    WranglerLogger.info("\nCreating scenario")
    my_scenario = Scenario.create_scenario(
        base_scenario=base_scenario,
        card_directory=my_config["scenario"]["card_directory"],
        tags=my_config["scenario"]["tags"],
        project_cards_list=project_cards_list,
        glob_search=my_config["scenario"]["glob_search"],
    )

    WranglerLogger.info(
        "\nApplying projects: {}".format("\n".join(my_scenario.get_project_names()))
    )
    print("Applying these projects to the base scenario ...")
    print("\n".join(my_scenario.get_project_names()))

    my_scenario.apply_all_projects()

    print("Creating model network...")
    WranglerLogger.info("\nCreating model network")
    model_road_net = ModelRoadwayNetwork.from_RoadwayNetwork(
        my_scenario.road_net, parameters=my_config.get("my_parameters", {})
    )
    WranglerLogger.info("\nCalculating additional variables and writing as shapefile")
    model_road_net.write_roadway_as_shp()
