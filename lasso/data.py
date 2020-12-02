
"""
Structures data overlaps as dataclasses with good defaults and helper methods. 

Example:
    
"""

import os
from dataclasses import dataclass
from typing import Any, Union, List, String, Dict, Tuple, Set, Mapping, Collection, Sequence

from pd import DataFrame
from gpd import GeoDataFrame


def update_df(base_df: DataFrame, update_df: DataFrame, merge_key: str, update_fields: Collection = [], overwrite: bool=False):
    """
    Updates specific fields of a dataframe with another dataframe using a key column.

    Args:
        base_df: DataFrame to be updated
        update_df: DataFrame with with updated values
        merge_key: column to merge on (i.e. model_link_id)
        update_fields: list of fields to update values for
        overwrite: if true, will copy over entire column. Otherwise, will only overwrite blank or NaN values.

    Returns: Dataframe with updated values
    """

    if not set(update_fields).issubset(base_df.columns):
        raise ValueError("Update fields: {} not in base_df: {}".format(update_fields,base_df.columns))
    if not set(update_fields).issubset(update_df.columns):
        raise ValueError("Update fields: {} not in update_df: {}".format(update_fields,update_df.columns))
    if not merge_key in base_df.columns:
        raise ValueError("Merge key: {} not in base_df: {}".format(merge_key, base_df.columns))
    if not merge_key in update_df.columns:
        raise ValueError("Merge key: {} not in update_df: {}".format(merge_key, update_df.columns))

    if overwrite:
      suffixes = ["-orig",None]
    else:
      base_df.loc[:,update_fields] = base_df.loc[:,update_fields].replace(r'^\s*$', np.nan, regex=True)
      suffixes = [None,"-update"]
    #print("base_df2:\n",base_df)
    merged_df = base_df.merge(
        update_df[update_fields+[merge_key]],
        on=merge_key,
        how="left",
        suffixes=suffixes,
    )
    #print("merged_df:\n",merged_df)
    if overwrite:
      merged_df = merged_df.drop(columns=[c+"-orig" for c in update_fields])
    else:
      for c in update_fields:
        #print(merged_df.apply(lambda row: row[c+"-update"] if not row[c] else row[c],axis=1))
        merged_df.loc[merged_df[c].isna(),c] = merged_df.loc[merged_df[c].isna(),c+"-update"]
      merged_df = merged_df.drop(columns=[c+"-update" for c in update_fields])
    #print("merged_df-updated:\n",merged_df)
    return merged_df

@dataclass
class FieldMapping:
    """A wrapper data class for mapping field renaming in a dataframe.

    Attributes:
        input_csv_filename: csv file with lookup values.
        input_csv_has_header: Boolean indicating if the input file has a header row.
        input_csv_fields: collection of length 2 indicating what fields to reference 
            for the (original field name, new field name).
        field_mapping: Mapping from original to target field names in a dataframe
    """
    input_csv_filename: str = None
    input_csv_has_header: bool = False
    input_csv_fields: Collection[Union[int,str]] = None
    field_mapping: Mapping[str,str] = {}

    def __post_init__(self):
        if self.input_csv_filename:
            if not len(self.input_csv_fields)==2:
                raise ValueError("Must specify which fields to reference for the original ane new field names")
            import csv
            self.read_csv_mapping()

    def self.read_csv_mapping()
        with open(self.input_csv_filename, mode='r') as infile:
            if self.input_csv_has_header: 
                reader = csv.DictReader(infile)
            else:
                reader = csv.reader(infile)
            for row in reader:
                if self.field_mapping.get(reader[self.input_csv_fields[0]]):
                    raise ValueError("Field rename csv {} has duplicate entry for field: {}".format(self.input_csv_filename, reader[self.input_csv_fields[0]]))
                self.field_mapping[reader[self.input_csv_fields[0]]] = reader[self.input_csv_fields[1]]]


@dataclass
class ValueLookup:
    """A data class for storing lookups dictionaries.

    Attributes:
        input_csv_filename: csv file with lookup values.
        input_csv_has_header: Boolean indicating if the input file has a header row.
        input_csv_key_field: Column with the lookup key in the csv file. Either the 
            field name (if has a header) or column number (starting at 1).
        target_df_key_field: Target dataframe field name with the lookup key.
        field_mapping: Mapping from the csv field (indicated by int or string) and 
            target df field.
        overwrite: If True, the variables will be overwritten. 
        value_mapping: Mapping of the target field name to a mapping of the key/values
    """
    input_csv_filename: str = None
    input_csv_has_header: bool = True
    input_csv_key_field: Union[str,int] = None
    target_df_key_field: str = None
    field_mapping: Mapping[Union[str,int],str] = {} # csv_field, output/target_field
    overwrite: bool = True
    mapping_df: DataFrame = None

    def __post_init__(self):
        for csv_field,df_field in self.field_mapping.items():
            if not self.value_mappings.get(df_field):
                self.value_mappings[df_field] = {}
        if self.input_csv_filename:
            self.read_csv_mapping()

    def self.read_csv_mapping():
        if not os.path.exists(self.input_csv_filename):
            raise ValueError("File {} does not exist but was specified as a value lookup file".format(self.input_csv_filename))
        self.mapping_df = pd.read_csv(
            self.input_csv_filename, 
            header=None if not self.input_csv_has_header else:header=0, 
            usecols=[self.input_csv_key_field]+list(self.field_mapping.keys()),
        )
    

    def self.apply_mapping(target_df, overwrite= None):
        """
        Apply a mapping to a pandas dataframe for a specific field.

        Args:
            target_df: pandas dataframe to apply mapping to. 

        Returns a merged dataframe.
        """

        if overwrite == None: overwrite = self.overwrite

        if overwrite:
            WranglerLogger.info(
                "Overwriting existing variables '{}' already in network".format(
                    df_field
                )
            )
            out_df = pd.merge(
                target_gdf,
                self.mapping_df.rename(
                    columns=self.field_mapping
                ),
                how="left",
                on_left=self.target_df_key_field,
                on_right=self.input_csv_key_field,
            )
            return out_df
        else:
            WranglerLogger.info(
                "Variable '{}' will be updated for some rows. Returning without overwriting for rows with values.".format(
                    df_field
                )
            )
        

@dataclass
class GeographicOverlay:
    shapefile_filename: str = None
    variable_mapping: Mapping[str,str] # target,overlay
    overwrite: bool = True
    gdf: GeoDataFrame = None
    added_id: str = '' #"LINK_ID"

    def __post_init__(self):
        if not os.path.exists(self.shapefile):
            raise ValueError("File {} does not exist but was specified as a geographic overlap file".format(self.shapefile))
        self.read_shapefile()
        if added_id:
            self.add_id(added_id)

    def self.read_shapefile_to_gdf(shapefile_filename: str = None):
        if not shapefile_filename: shapefile_filename = self.shapefile_filename
        self.gdf = gpd.read_file(mrcc_roadway_class_shape)
        WranglerLogger.debug("Read shapefile {} with columns\n{}".format(shapefile_filename,self.gdfcolumns))

    def self.add_id(added_id)
        gdf[added_id] = range(1, 1 + len(self.gdf))


