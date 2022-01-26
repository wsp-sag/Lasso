import numpy as np
import pandas as pd
import pytest

from network_wrangler import WranglerLogger

from lasso.utils import profile_me


def test_get_shared_streets_intersection_hash():
    from lasso.utils import get_shared_streets_intersection_hash

    hash_result = get_shared_streets_intersection_hash(
        lat=-93.0965985, lon=44.952112199999995, osm_node_id=954734870
    )

    hash_expected = "69f13f881649cb21ee3b359730790bb9"

    assert hash_result == hash_expected


@pytest.mark.ci
def test_column_name_to_parts():
    from lasso.utils import column_name_to_parts
    from lasso.parameters import Parameters

    parameters = Parameters()

    result_1 = column_name_to_parts("ML_LANES_AM_HOV", parameters=parameters)
    result_2 = column_name_to_parts("LANES_PM", parameters=parameters)
    result_3 = column_name_to_parts("ML_PRICE_NT", parameters=parameters)

    # base_name, time_period, category, managed
    assert ("LANES", "AM", "HOV", True) == result_1
    assert ("LANES", "PM", None, False) == result_2
    assert ("PRICE", "NT", None, True) == result_3


@pytest.mark.ci
def test_fill_df_na():
    from lasso.utils import fill_df_na
    from pandas import DataFrame, NA
    from pandas._testing import assert_frame_equal

    type_lookup = {
        "i_am_string": str,
        "i_am_float": float,
        "i_am_int": int,
    }

    df_w_empties = DataFrame(
        {
            "i_am_string": ["hi", NA],
            "i_am_float": [1.0, 4.0],
            "i_am_int": [1000, NA],
        },
    )
    result_1 = fill_df_na(df_w_empties, type_lookup)

    df_expected = DataFrame(
        {
            "i_am_string": ["hi", ""],
            "i_am_float": [1.0, 4.0],
            "i_am_int": [1000, 0],
        },
    )
    assert_frame_equal(result_1, df_expected)


@pytest.mark.ci
def test_coerce_df_types():
    from lasso.utils import coerce_df_types
    from pandas._testing import assert_frame_equal

    type_lookup = {
        "i_am_string": str,
        "i_am_float": float,
        "i_am_int": int,
    }

    df_mistyped = pd.DataFrame(
        {
            "i_am_string": ["hi", np.nan],
            "i_am_float": ["4.6", pd.NA],
            "i_am_int": [1000, "0"],
        },
    )

    result_1 = coerce_df_types(df_mistyped, type_lookup)

    df_expected = pd.DataFrame(
        {
            "i_am_string": ["hi", ""],
            "i_am_float": [4.6, 0.0],
            "i_am_int": [1000, 0],
        },
    )
    assert_frame_equal(result_1, df_expected)


def test_profiler():
    @profile_me
    def _hi():
        def _mult(x):
            return x ^ 2

        for x in range(10):
            return _mult(x)


def test_select_from_df():
    from lasso.utils import select_df_from_df
    from numpy import array_equal

    select_df = pd.DataFrame({"a": list("ABC"), "b": list("CDF")})
    df = pd.DataFrame({"a": list("ABAC"), "b": list("CFFF")})
    compare_cols = ["a", "b"]
    correct_result_df = pd.DataFrame({"a": list("AC"), "b": list("CF")})
    result_df = select_df_from_df(df, select_df, compare_cols=compare_cols)
    array_equal(correct_result_df.values, result_df.values)

@pytest.mark.now
@pytest.mark.ci
def test_intersect_dfs():
    df1 = pd.DataFrame(
        {
            "my_id": [1, 2, 3],
            "A": ["a1-base", "a2", "a3-deleted"],
            "B": ["b1", "b2", "b3-deleted"],
            "C": ["c1", "c2", "c3-deleted"],
        }
    )
    df2 = pd.DataFrame(
        {
            "my_id": [1, 2, 4],
            "A": ["a1-changed", "a2", "a4-added"],
            "B": ["b1", "b2", "b4-added"],
            "D": ["d1-added", "d2-added", "d4-added"],
        }
    )
    id_col = "my_id"

    from lasso.utils import intersect_dfs
    from pandas._testing import assert_frame_equal

    # inner join
    result_inner_df1, result_inner_df2 = intersect_dfs(
        df1,
        df2,
        how_cols_join="inner",
        how_rows_join="inner",
        id_col="my_id",
    )
    expected_inner_df1 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-base", "a2"],
            "B": ["b1", "b2"],
        }
    )
    expected_inner_df2 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-changed", "a2"],
            "B": ["b1", "b2"],
        }
    )

    WranglerLogger.debug(f"result_inner_df1:\n{result_inner_df1}")
    WranglerLogger.debug(f"expected_inner_df1:\n{expected_inner_df1}")

    WranglerLogger.debug(f"result_inner_df2:\n{result_inner_df2}")
    WranglerLogger.debug(f"expected_inner_df2:\n{expected_inner_df2}")

    assert_frame_equal(result_inner_df1, expected_inner_df1)
    assert_frame_equal(result_inner_df2, expected_inner_df2)

    # left join
    result_left_df1, result_left_df2 = intersect_dfs(
        df1,
        df2,
        how_cols_join="left",
        how_rows_join="inner",
        id_col="my_id",
    )
    expected_left_df1 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-base", "a2"],
            "B": ["b1", "b2"],
            "C": ["c1", "c2"],
        }
    )
    expected_left_df2 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-changed", "a2"],
            "B": ["b1", "b2"],
            "C": [np.nan, np.nan],
        }
    )

    WranglerLogger.debug(f"result_left_df1:\n{result_left_df1}")
    WranglerLogger.debug(f"expected_left_df1:\n{expected_left_df1}")

    WranglerLogger.debug(f"result_left_df2:\n{result_left_df2}")
    WranglerLogger.debug(f"expected_left_df2:\n{expected_left_df2}")

    assert_frame_equal(result_left_df1, expected_left_df1)
    assert_frame_equal(result_left_df2, expected_left_df2)

    # right join
    result_right_df1, result_right_df2 = intersect_dfs(
        df1,
        df2,
        how_cols_join="right",
        how_rows_join="inner",
        id_col="my_id",
    )
    expected_right_df1 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-base", "a2"],
            "B": ["b1", "b2"],
            "D": [np.nan, np.nan],
        }
    )
    expected_right_df2 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-changed", "a2"],
            "B": ["b1", "b2"],
            "D": ["d1-added", "d2-added"],
        }
    )

    WranglerLogger.debug(f"result_right_df1:\n{result_right_df1}")
    WranglerLogger.debug(f"expected_left_df1:\n{expected_right_df1}")

    WranglerLogger.debug(f"result_right_df2:\n{result_right_df2}")
    WranglerLogger.debug(f"expected_right_df2:\n{expected_right_df2}")

    assert_frame_equal(result_right_df1, expected_right_df1)
    assert_frame_equal(result_right_df2, expected_right_df2)

    # outer join
    result_outer_df1, result_outer_df2 = intersect_dfs(
        df1,
        df2,
        how_cols_join="outer",
        how_rows_join="inner",
        id_col="my_id",
    )
    expected_outer_df1 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-base", "a2"],
            "B": ["b1", "b2"],
            "C": ["c1", "c2"],
            "D": [np.nan, np.nan],
        }
    )
    expected_outer_df2 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-changed", "a2"],
            "B": ["b1", "b2"],
            "C": [np.nan, np.nan],
            "D": ["d1-added", "d2-added"],
        }
    )

    WranglerLogger.debug(f"result_outer_df1:\n{result_outer_df1}")
    WranglerLogger.debug(f"expected_outer_df1:\n{expected_outer_df1}")

    WranglerLogger.debug(f"result_outer_df2:\n{result_outer_df2}")
    WranglerLogger.debug(f"expected_outer_df2:\n{expected_outer_df2}")

    assert_frame_equal(result_outer_df1, expected_outer_df1)
    assert_frame_equal(result_outer_df2, expected_outer_df2)

    # selected inner join
    select_cols = ["A"]
    result_sel_inner_df1, result_sel_inner_df2 = intersect_dfs(
        df1,
        df2,
        how_cols_join="inner",
        how_rows_join="inner",
        id_col="my_id",
        select_cols=select_cols,
    )
    expected_sel_inner_df1 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-base", "a2"],
        }
    )
    expected_sel_inner_df2 = pd.DataFrame(
        {
            "my_id": [1, 2],
            "A": ["a1-changed", "a2"],
        }
    )

    WranglerLogger.debug(f"result_sel_inner_df1:\n{result_sel_inner_df1}")
    WranglerLogger.debug(f"expected_sel_inner_df1:\n{expected_sel_inner_df1}")

    WranglerLogger.debug(f"result_sel_inner_df2:\n{result_sel_inner_df2}")
    WranglerLogger.debug(f"expected_sel_inner_df2:\n{expected_sel_inner_df2}")

    assert_frame_equal(result_sel_inner_df1, expected_sel_inner_df1)
    assert_frame_equal(result_sel_inner_df2, expected_sel_inner_df2)


@pytest.mark.menow
@pytest.mark.ci
def test_find_df_changes():
    df1 = pd.DataFrame(
        {
            "my_id": [1, 2, 3],
            "A": ["a1-base", "a2", "a3-deleted"],
            "B": ["b1", "b2", "b3-deleted"],
            "C": ["c1", "c2", "c3-deleted"],
        }
    )
    df2 = pd.DataFrame(
        {
            "my_id": [1, 2, 4],
            "A": ["a1-changed", "a2", "a4-added"],
            "B": ["b1", "b2", "b4-added"],
            "D": ["d1-added", "d2-added", "d4-added"],
        }
    )

    expected_changes_df = expected_inner_df2 = pd.DataFrame(
        {
            ("my_id","self"): [1, 2],
            ("A","self"): ["a1-base", "a2"],
            ("A","other"): ["a1-changed", "a2"],
            ("B","self"): ["b1", "b2"],
            ("B","other"): ["b1", "b2"],
            ("D","self"): [np.nan,np.nan],
            ("D","other"): ["d1-added", "d2-added"],
        }
    )

    id_col = "my_id"

    from lasso.utils import find_df_changes
    from pandas._testing import assert_frame_equal

    # inner join
    changes_df = find_df_changes(
        df1,
        df2,
        select_records="updated",
        id_col="my_id",
    )

    WranglerLogger.debug(f"changes_df:\n{changes_df}")
    WranglerLogger.debug(f"expected_changes_df1:\n{expected_changes_df}")

    assert_frame_equal(changes_df, expected_changes_df)
