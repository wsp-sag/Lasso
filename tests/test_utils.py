import pytest

@pytest.mark.utils
@pytest.mark.travis
def test_update_crs_nodes_df(request):
    """
    Tests that parameters are read
    """
    print("\n--Starting:", request.node.name)   
    from lasso.util import update_crs_nodes_df
    import pandas as pd
    import geopandas as gpd

    # From GeoPandas Documentation

    base_df = pd.DataFrame(
        {'City': ['St. Paul','Deluth','Bemidji'],
        'Y': [44.9537, 46.7867,47.4716],
        'X': [-93.0900,-92.1005, 94.8827]}) 

    base_gdf = gpd.GeoDataFrame(
        base_df, geometry=gpd.points_from_xy(base_df.X, base_df.Y)
    )

    test_df1 = update_crs_nodes_df(base_df,from_crs=4326,to_crs=26831)
    test_df2 = update_crs_nodes_df(base_gdf,from_crs=4326,to_crs=26831)


    try:
        pd.testing.assert_frame_equal(test_df1,test_df2)
    except AssertionError as e:
        print(f"test_df1:\n{test_df1}") 
        print(f"test_df1:\n{test_df2}") 
        raise AssertionError(f"GeoDataFrame and DataFrame produce different results\n{e}")
    
    answer_df = pd.DataFrame(
        {'City': ['St. Paul','Deluth','Bemidji'],
        'Y': [95523.14,300814.7,9651105.],
        'X': [891552.5,964104.6,-4173595.]}) 

    # is it translating coordinate system correctly?
    try:
        pd.testing.assert_frame_equal(test_df1,answer_df,check_exact=False,atol=1e-3)
    except AssertionError as e:
        print(f"test_df1:\n{test_df1}") 
        print(f"answer_df:\n{answer_df}") 
        raise AssertionError(f"Coordinate reference conversion failed\n{e}")

    test_df3 = update_crs_nodes_df(base_gdf,from_crs=4326,to_crs=26831,keep_gdf=False)
    try:
        pd.testing.assert_frame_equal(test_df3,answer_df,check_exact=False,atol=1e-3)
    except AssertionError as e:
        print(f"test_df1:\n{test_df1}") 
        print(f"answer_df:\n{answer_df}") 
        raise AssertionError(f"Coordinate reference conversion failed when using a geodataframe\n{e}")

    
    
