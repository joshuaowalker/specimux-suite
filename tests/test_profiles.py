"""Profiles: the bundled ones always load, a user profile's version pin is
honoured."""

import pytest

from specimux_suite import __version__
from specimux_suite.profiles import ProfileVersionError, SuiteProfile, _check_version_compatible, list_profiles


@pytest.mark.parametrize("name", ["default", "herbarium"])
def test_bundled_profiles_load_under_the_installed_version(name):
    """Regression: the bundled files once pinned 0.1.* and `--profile
    default` refused to load on 0.2.1."""
    assert name in list_profiles()
    profile = SuiteProfile.load(name)                  # check_version defaults to True
    assert profile.name == name
    assert _check_version_compatible(profile.version, __version__)


def test_a_user_profile_with_a_stale_pin_is_refused(tmp_path):
    path = tmp_path / "old.yaml"
    path.write_text('specimux-suite-version: "0.1.*"\ndescription: "old"\nsuite:\n  min-reads: 5\n')
    with pytest.raises(ProfileVersionError):
        SuiteProfile._load_from_path(path, "old", check_version=True)
    assert SuiteProfile._load_from_path(path, "old", check_version=False).suite["min-reads"] == 5


def test_version_patterns():
    assert _check_version_compatible("*", "0.3.0")
    assert _check_version_compatible("0.3.*", "0.3.7")
    assert not _check_version_compatible("0.1.*", "0.3.0")
    assert _check_version_compatible("0.3.0", "0.3.0") and not _check_version_compatible("0.3.0", "0.3.1")
