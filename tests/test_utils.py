import pytest
from lasso.utils import profile_me


def test_get_shared_streets_intersection_hash():
    from lasso.utils import get_shared_streets_intersection_hash

    hash_result = get_shared_streets_intersection_hash(
        lat=-93.0965985, lon=44.952112199999995, osm_node_id=954734870
    )

    hash_expected = "69f13f881649cb21ee3b359730790bb9"

    assert hash_result == hash_expected


@pytest.mark.travis
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


@pytest.mark.travis
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
        {"i_am_string": ["hi", NA], "i_am_float": [1.0, 4.0], "i_am_int": [1000, NA],},
    )
    result_1 = fill_df_na(df_w_empties, type_lookup)

    df_expected = DataFrame(
        {"i_am_string": ["hi", ""], "i_am_float": [1.0, 4.0], "i_am_int": [1000, 0],},
    )
    assert_frame_equal(result_1, df_expected)


@pytest.mark.travis
def test_coerce_df_types():
    from lasso.utils import coerce_df_types
    from pandas import DataFrame, NA
    from numpy import nan
    from pandas._testing import assert_frame_equal

    type_lookup = {
        "i_am_string": str,
        "i_am_float": float,
        "i_am_int": int,
    }

    df_mistyped = DataFrame(
        {
            "i_am_string": ["hi", nan],
            "i_am_float": ["4.6", NA],
            "i_am_int": [1000, "0"],
        },
    )

    result_1 = coerce_df_types(df_mistyped, type_lookup)

    df_expected = DataFrame(
        {"i_am_string": ["hi", ""], "i_am_float": [4.6, 0.0], "i_am_int": [1000, 0],},
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
    import pandas as pd
    from lasso.utils import select_df_from_df
    from numpy import array_equal

    select_df = pd.DataFrame({"a": list("ABC"), "b": list("CDF")})
    df = pd.DataFrame({"a": list("ABAC"), "b": list("CFFF")})
    compare_cols = ["a", "b"]
    correct_result_df = pd.DataFrame({"a": list("AC"), "b": list("CF")})
    result_df = select_df_from_df(df, select_df, compare_cols=compare_cols)
    array_equal(correct_result_df.values, result_df.values)
