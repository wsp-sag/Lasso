import os
from typing import Collection

from pandas import DataFrame

from lasso.model_roadway import ModelRoadwayNetwork
from lasso.utils import check_overwrite


def write_roadway_as_fixedwidth_with_cube(
    net: ModelRoadwayNetwork,
    links_df: DataFrame,
    nodes_df: DataFrame,
    node_output_fields: Collection[str] = None,
    link_output_fields: Collection[str] = None,
    output_directory: str = None,
    output_prefix: str = None,
    output_basename_links: str = None,
    output_basename_nodes: str = None,
    overwrite_existing_output: bool = False,
    build_script: str = True,
) -> None:
    """Writes out fixed width files, headers, and build script.

    This function does:
    1. write out link and node fixed width data files for cube.
    2. write out header and width correspondence.
    3. write out build script with header and width specification based
        on format specified.

    Args:
        links_df (GeoDataFrame, optional): The links file to be output. If not specified,
            will default to self.model_links_df.
        nodes_df (GeoDataFrame, optional): The modes file to be output. If not specified,
            will default to self.nodes_df.
        node_output_fields (Collection[str], optional): List of strings for node
            output variables. Defaults to parameters.roadway_network_ps.output_fields.
        link_output_fields (Collection[str], optional): List of strings for link
            output variables. Defaults to parameters.roadway_network_ps.output_fields.
        output_directory (str, optional): If set, will combine with output_link_shp and
            output_node_shp to form output directory. Defaults to
            parameters.file_ps.output_directory, which defaults to "".
        output_prefix (str, optional): prefix to add to output files. Helpful for
            identifying a scenario.
            Defaults to parameters.file_ps.output_prefix, which defaults to "".
        output_basename_links (str, optional): Combined with the output_director,
            output_prefix, and the appropriate filetype suffix for the
            link output filenames. Defaults to parameters.file_ps.output_basename_links,
            which defaults to  "links_out".
        output_basename_nodes (str, optional): Combined with the output_director,
            output_prefix, and
            the appropriate filetype suffix for the node output filenames.
            Defaults to parameters.file_ps.output_basename_nodes, which defaults to
            "links_out".
        overwrite_existing_output (bool, optional): if True, will not ask about overwriting
            existing output. Defaults to False.
        build_script (str, optional): If True, will output a script to the output
            directory which will rebuild the network as a HWYNET Cube Script.
            Defaults to True.
    """
    _link_header_df, _node_header_df = net.write_roadway_as_fixedwidth(
        links_df,
        nodes_df,
        node_output_fields,
        link_output_fields,
        output_directory,
        output_prefix,
        output_basename_links,
        output_basename_nodes,
        overwrite_existing_output,
    )

    if build_script:
        _outfile_build_script = os.path.join(
            output_directory,
            output_prefix + "_build_cube_hwynet.s",
        )

        write_cube_hwy_net_script_network_from_ff_files(
            _link_header_df,
            _node_header_df,
            script_outfile=_outfile_build_script,
            overwrite=True,
        )


def write_cube_hwy_net_script_network_from_ff_files(
    links_df: DataFrame,
    nodes_df: DataFrame,
    link_header_df: DataFrame,
    node_header_df: DataFrame,
    script_outfile: str = "build_network_from_ff_s",
    overwrite: bool = False,
) -> None:
    """Writes the cube script to read a network written to a fixed-format file to cube.

    Args:
        links_df (DataFrame): Dataframe with link values.
        nodes_df (DataFrame): Dataframe with node values.
        link_header_df (DataFrame): Dataframe with a row for each link field and
            columns "header", "width"
        node_header_df (DataFrame): ataframe with a row for each link field and
            columns "header", "width"
        script_outfile (str, optional): Script filename. Defaults to "build_network_from_ff.s".
        overwrite (bool, optional): Defaults to False.
    """

    if not overwrite:
        check_overwrite(script_outfile)

    s = 'RUN PGM = NETWORK MSG = "Read in network from fixed width file" \n'
    s += "FILEI LINKI[1] = %LINK_DATA_PATH%,"
    start_pos = 1
    for i in range(len(link_header_df)):
        s += " VAR=" + link_header_df.header.iloc[i]

        if links_df.dtypes.loc[link_header_df.header.iloc[i]] == "O":
            s += "(C" + str(link_header_df.width.iloc[i]) + ")"

        s += (
            ", BEG="
            + str(start_pos)
            + ", LEN="
            + str(link_header_df.width.iloc[i])
            + ","
        )

        start_pos += link_header_df.width.iloc[i] + 1

    s = s[:-1]
    s += "\n"
    s += "FILEI NODEI[1] = %NODE_DATA_PATH%,"
    start_pos = 1
    for i in range(len(node_header_df)):
        s += " VAR=" + node_header_df.header.iloc[i]

        if nodes_df.dtypes.loc[node_header_df.header.iloc[i]] == "O":
            s += "(C" + str(node_header_df.width.iloc[i]) + ")"

        s += (
            ", BEG="
            + str(start_pos)
            + ", LEN="
            + str(node_header_df.width.iloc[i])
            + ","
        )

        start_pos += node_header_df.width.iloc[i] + 1

    s = s[:-1]
    s += "\n"
    s += 'FILEO NETO = "%SCENARIO_DIR%/complete_network.net" \n\n    ZONES = %zones% \n\n'
    s += "ROADWAY = LTRIM(TRIM(ROADWAY)) \n"
    s += "NAME = LTRIM(TRIM(NAME)) \n"
    s += "\n \nENDRUN"

    with open(script_outfile, "w") as f:
        f.write(s)
