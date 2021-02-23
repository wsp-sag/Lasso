# source
[MNDOT ftp](ftp://ftp.metc.state.mn.us/NetworkRebuild/ Wisconsin_Lanes_Counts_Median .zip)


# Count Data Files

 - **`TRADAS_(counts).shp`**: Shapefile with following fields:
   - `OBJECTID`  
   - `TRADS_ID`  
   - `AADT_RPTG_`    
   - `RDWY_LAST_` 
   - `RDWY_AADT`  ## @i-am-sijia please confirm this is field used in wiAADT?
   - `PDIR_AADT`  
   - `NDIR_AADT`  
   - `TRFC_PT_LU`  
   - `TRFC_PT_LO`  
 - **`wi_count_ShSt_API_match.csv`**: File used in Lasso with the following fields:
    - `AADT_wi`
    - `originalFeatureId` ## @i-am-sijia please confirm what field this maps to
    - `shstReferenceId` 

# Functional Class Files
 - **`WISLR.shp`**: 
 - **`WISLR_with_id.shp`**: 
 - **`widot.out.matched.geojson`** 