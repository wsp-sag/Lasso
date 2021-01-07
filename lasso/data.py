"""
Structures data overlaps as dataclasses with good defaults and helper methods. 

Example:
    
"""

import os
from dataclasses import dataclass, field
from typing import Any, Union, List, Dict, Tuple, Set, Mapping, Collection, Sequence

import numpy as np
import pandas as pd
import geopandas as gpd
from pandas import DataFrame
from geopandas import GeoDataFrame

from network_wranger import update_df

from .logger import WranglerLogger

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
    input_csv_fields: Collection[Union[int, str]] = None
    field_mapping: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.input_csv_filename:
            if not len(self.input_csv_fields) == 2:
                raise ValueError(
                    "Must specify which fields to reference for the original ane new field names"
                )
            self.read_csv_mapping()

    def read_csv_mapping(self):
        import csv

        with open(self.input_csv_filename, mode="r") as infile:
            if self.input_csv_has_header:
                reader = csv.DictReader(infile)
            else:
                reader = csv.reader(infile)
            for row in reader:
                if self.field_mapping.get(row[self.input_csv_fields[0]]):
                    raise ValueError(
                        "Field rename csv {} has duplicate entry for field: {}".format(
                            self.input_csv_filename,
                            row[self.input_csv_fields[0]],
                        )
                    )
                self.field_mapping[row[self.input_csv_fields[0]]] = row[
                    self.input_csv_fields[1]
                ]


@dataclass
class ValueLookup:
    """A data class for storing lookups dictionaries.

    Attributes:
        input_csv_filename: csv file with lookup values.
        input_geojson_filename: geojson file with lookup values.
        input_dbf_filename: dbf file with lookup values. 
        input_csv_has_header: Boolean indicating if the input file has a header row.
        input_key_field: Column with the lookup key in the csv file. Either the
            field name (if has a header) or column number (starting at 1).
        target_df_key_field: Target dataframe field name with the lookup key.
        field_mapping: Mapping from the csv field (indicated by int or string) and
            target df field.
        update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
            "update if found", or "update nan". Defaults to "update if found"
        value_mapping: Mapping of the target field name to a mapping of the key/values
    """

    input_csv_filename: str = None
    input_csv_has_header: bool = True
    input_geojson_filename: str = None
    input_dbf_filename: str = None
    input_key_field: Union[str, int] = None
    target_df_key_field: str = None
    field_mapping: Mapping[Union[str, int], str] = field(
        default_factory=dict
    )  # input csv/json_field, output/target_field
    update_method: str = "update if found"
    mapping_df: DataFrame = None

    def __post_init__(self):
        input_files = [self.input_csv_filename,self.input_geojson_filename, self.input_dbf_filename]
        if input_files.count(None)<2:
            msg = "ValueLookup should have input_csv_filename OR input_geojson_filename OR input_dbf_filename but >1 specified: {}".format(input_files)
            WranglerLogger.error(msg)
            ValueError(msg)

        self.check_fields()

    def check_fields(self):
        """
        Read header of mapping file and check to make sure key field and mapping fields
        are columns.
        """
        if self.input_csv_filename and self.input_csv_has_header:
            cols = pd.read_csv(self.input_csv_filename,nrows=0).columns.tolist()
        elif self.input_geojson_filename:
            cols = gpd.read_file(self.input_geojson_filename,nrows=0).columns.tolist()
        elif self.input_dbf_filename:
            cols = gpd.read_file(self.input_dbf_filename,nrows=0).columns.tolist()
        else:
            raise ValueError("Must have an input file for mapping, none found")

        
        _input_file_name = next((x for x in [self.input_csv_filename,self.input_geojson_filename, self.input_dbf_filename] if x is not None),"No filename")

        _necessary_fields = set([self.input_key_field] + list(self.field_mapping.keys()))
        _missing_csv_fields = list(set(_necessary_fields) - set(cols))

        if _missing_csv_fields:
            raise ValueError(
                "Missing the following requiredfields in the lookup GEOJSON/CSV/DBF file {},\
                which are specified: {}".format(
                    _input_file_name, _missing_csv_fields
                )
            )

    def read_mapping(self):
        if self.input_csv_filename:
            self.read_csv_mapping()
        elif self.input_geojson_filename:
            self.read_mapping_geopandas(self.input_geojson_filename)
        elif self.input_dbf_filename:
            self.read_mapping_geopandas(self.input_dbf_filename)
        else:
            WranglerLogger.warning("Value Mapping has no input file to read as a mapping")

        
    def read_mapping_geopandas(self, input_filename):
        if not os.path.exists(input_filename):
            raise ValueError(
                "File {} does not exist but was specified as a value lookup file".format(
                    self.input_geojson_filename
                )
            )

        gdf = gpd.read_file(input_filename)

        if self.field_mapping:
            usecols=[self.input_key_field] + list(self.field_mapping.keys())
            self.mapping_df = gdf[usecols]
        else:
            self.mapping_df = gdf


    def read_csv_mapping(self):
        if not os.path.exists(self.input_csv_filename):
            raise ValueError(
                "File {} does not exist but was specified as a value lookup file".format(
                    self.input_csv_filename
                )
            )
        if not self.input_csv_has_header:
            _h = None
        else:
            _h = 0

        if self.field_mapping:
            self.mapping_df = pd.read_csv(
                self.input_csv_filename,
                header=_h,
                usecols=[self.input_key_field] + list(self.field_mapping.keys()),
            )
        else:
            self.mapping_df = pd.read_csv(
                self.input_csv_filename,
                header=_h,
            )

    def apply_mapping(self, target_df: DataFrame, update_method: str = None):
        """
        Apply a mapping to a pandas dataframe for a specific field.

        Args:
            target_df: pandas DataFrame to apply mapping to.
            update_method: update method to use in network_wrangler.update_df. One of "overwrite all", 
            "update if found", or "update nan". Defaults to class instance value which defaults to "update if found"

        Returns a merged DataFrame.
        """
        if self.mapping_df == None:
            self.read_mapping()

        if update_method == None:
            update_method = self.update_method

        overwrite_fields = [v for v in list(self.field_mapping.values()) if v in target_df.columns]
        out_df = update_df(
            target_df, 
            self.mapping_df.rename(columns=self.field_mapping), 
            left_on=self.target_df_key_field,
            right_on=self.input_key_field, 
            update_fields =  list(self.field_mapping.values),
            method = update_method,
        )

@dataclass
class PolygonOverlay:
    input_filename: str = None
    field_mapping: Mapping[str, str] = field(default_factory=dict)  # target,overlay
    overwrite: bool = True
    gdf: GeoDataFrame = None
    added_id: str = ""  # "LINK_ID"

    def __post_init__(self):
        if not os.path.exists(self.input_filename):
            raise ValueError(
                "File {} does not exist but was specified as a geographic overlap file".format(
                    self.shapefile_filename
                )
            )
        self.read_shapefile_to_gdf()
        if self.added_id:
            self.add_id(self.added_id)

    def read_file_to_gdf(self, input_filename: str = None):
        if not input_filename:
            input_filename = self.input_filename
        self.gdf = gpd.read_file(input_filename)
        WranglerLogger.debug(
            "Read file {} with columns\n{}".format(
                input_filename, self.gdf.columns
            )
        )

    def add_id(self, added_id):
        self.gdf[added_id] = range(1, 1 + len(self.gdf))
