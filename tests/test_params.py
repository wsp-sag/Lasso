import pytest


@pytest.mark.params
@pytest.mark.travis
def test_update_params(request):
    print("\n--Starting:", request.node.name)
    from lasso import Parameters

    p = Parameters()
    p.update(update_dict={"demand_time_periods": ["A", "B"]})
    assert p.demand_model_ps.demand_time_periods == ["A", "B"]

    p.update(output_link_fixed_width_header_filename="hellothere.txt")
    assert p.file_ps.output_link_fixed_width_header_filename == "hellothere.txt"


@pytest.mark.params
@pytest.mark.travis
def test_params_as_dict(request):
    print("\n--Starting:", request.node.name)
    from lasso import Parameters

    p1 = Parameters(input_ps={"demand_time_periods": ["A", "B"]})
    p1_dict = p1.as_dict()
    print("parameters.as_dict():{}\n   ".format(p1_dict))
    p2 = Parameters(input_ps=p1_dict)
    p2_dict = p2.as_dict()

    assert p1_dict == p2_dict
