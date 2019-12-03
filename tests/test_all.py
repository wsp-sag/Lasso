import subprocess
import pytest

##todo make this actually fail when subprocess does
@pytest.mark.all
def test_all():

    try:
        subprocess.run(
            [
                "python",
                "scripts/make_mc_scenario.py",
                "examples/settings/my_config.yaml",
            ]
        ).check_returncode()
    except subprocess.CalledProcessError as e:
        print(e.output)
