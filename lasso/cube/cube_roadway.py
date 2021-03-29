import os

from pandas import DataFrame


def write_cube_hwy_net_script_network_from_ff_files(
    links_df: DataFrame,
    nodes_df: DataFrame,
    link_header_df: DataFrame,
    node_header_df: DataFrame,
    script_outfile: str = "build_network_from_ff_s",
    overwrite: bool = False,
) -> None:
    """[summary]

    Args:
        links_df (DataFrame): [description]
        nodes_df (DataFrame): [description]
        link_header_df (DataFrame): [description]
        node_header_df (DataFrame): [description]
        script_outfile (str, optional): [description]. Defaults to "build_network_from_ff_s".
        overwrite (bool, optional): Defaults to False.
    """

    if not overwrite:
        if os.path.exists(script_outfile):
            raise ValueError(
                "outfile: {} already exists and overwrite set to False.".format(
                    script_outfile
                )
            )

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
