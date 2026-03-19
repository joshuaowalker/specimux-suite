"""Suite-level profile system for specimux-suite.

Suite profiles compose tool profiles by reference. They reference tool profiles
by name — the suite passes `-p <name>` to the subprocess, delegating resolution
to the tool's own profile system.

Profile resolution order:
1. User profiles in ~/.config/specimux-suite/profiles/
2. Bundled profiles in package

Override order: defaults -> profile -> explicit CLI arguments
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import logging
import re

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

try:
    from specimux_suite import __version__
except ImportError:
    __version__ = "dev"

# Use importlib.resources for Python 3.9+
HAS_IMPORTLIB_RESOURCES = False

try:
    from importlib.resources import files as importlib_files
    from importlib.resources import as_file
    HAS_IMPORTLIB_RESOURCES = True
except ImportError:
    pass

logger = logging.getLogger(__name__)

# XDG-compliant config path
XDG_CONFIG_HOME = Path.home() / ".config"
PROFILES_DIR = XDG_CONFIG_HOME / "specimux-suite" / "profiles"

VALID_SUITE_KEYS = {
    "min-reads",
    "reprocess-ratio",
    "workers",
    "live-presample",
}

VALID_IDENTIFY_KEYS = {
    "vsearch-min-identity",
    "vsearch-max-accepts",
    "identify-min-coverage",
}


class ProfileError(Exception):
    """Error loading or applying a profile."""
    pass


class ProfileVersionError(ProfileError):
    """Profile version is incompatible with current specimux-suite version."""
    pass


class ProfileValidationError(ProfileError):
    """Profile contains invalid keys."""
    pass


@dataclass
class SuiteProfile:
    """A suite-level profile composing tool profiles and pipeline settings."""
    name: str
    version: str
    description: str
    suite: dict[str, Any] = field(default_factory=dict)
    specimux: dict[str, Any] = field(default_factory=dict)
    speconsense: dict[str, Any] = field(default_factory=dict)
    summarize: dict[str, Any] = field(default_factory=dict)
    identify: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, name: str, check_version: bool = True) -> 'SuiteProfile':
        """Load profile by name from user dir or bundled.

        Args:
            name: Profile name (without .yaml extension)
            check_version: If True, validate version compatibility

        Returns:
            Loaded SuiteProfile instance

        Raises:
            ProfileError: If profile not found or YAML unavailable
            ProfileVersionError: If version incompatible
            ProfileValidationError: If profile contains invalid keys
        """
        if yaml is None:
            raise ProfileError(
                "PyYAML is required for profile support. "
                "Install with: pip install pyyaml"
            )

        # Try user profile first
        user_path = PROFILES_DIR / f"{name}.yaml"
        if user_path.exists():
            return cls._load_from_path(user_path, name, check_version)

        # Fall back to bundled profile
        bundled_path = _get_bundled_profile_path(name)
        if bundled_path is not None:
            return cls._load_from_path(bundled_path, name, check_version)

        # Profile not found - provide helpful error
        available = list_profiles()
        if available:
            raise ProfileError(
                f"Profile '{name}' not found. Available profiles: {', '.join(available)}"
            )
        else:
            raise ProfileError(
                f"Profile '{name}' not found and no profiles are available. "
                f"Check that profiles are installed in {PROFILES_DIR}"
            )

    @classmethod
    def _load_from_path(cls, path: Path, name: str, check_version: bool) -> 'SuiteProfile':
        """Load profile from a specific path."""
        try:
            with open(path, 'r') as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ProfileError(f"Invalid YAML in profile '{name}': {e}")
        except IOError as e:
            raise ProfileError(f"Cannot read profile '{name}': {e}")

        if not isinstance(data, dict):
            raise ProfileError(f"Profile '{name}' must be a YAML mapping")

        # Extract fields
        version = data.get('specimux-suite-version', '*')
        description = data.get('description', '')
        suite = data.get('suite', {}) or {}
        specimux = data.get('specimux', {}) or {}
        speconsense = data.get('speconsense', {}) or {}
        summarize = data.get('summarize', {}) or {}
        identify = data.get('identify', {}) or {}

        # Validate version compatibility
        if check_version and not _check_version_compatible(version, __version__):
            raise ProfileVersionError(
                f"Profile '{name}' requires specimux-suite version {version}, "
                f"but you have {__version__}.\n\n"
                f"This profile may use parameters that have changed or been removed.\n"
                f"Please update the profile for your version."
            )

        # Validate suite and identify keys (specimux/speconsense are pass-through)
        _validate_keys(name, suite, identify)

        return cls(
            name=name,
            version=version,
            description=description,
            suite=suite,
            specimux=specimux,
            speconsense=speconsense,
            summarize=summarize,
            identify=identify,
        )


def _check_version_compatible(profile_version: str, current_version: str) -> bool:
    """Check if profile version pattern matches current version.

    Supports wildcards: "0.1.*" matches "0.1.0", "0.1.1", etc.
    """
    if profile_version == '*':
        return True

    pattern = profile_version.replace('.', r'\.').replace('*', r'.*')
    pattern = f'^{pattern}$'

    try:
        return bool(re.match(pattern, current_version))
    except re.error:
        return profile_version == current_version


def _validate_keys(name: str, suite: dict, identify: dict) -> None:
    """Validate that suite and identify sections only contain known keys."""
    unknown_suite = set(suite.keys()) - VALID_SUITE_KEYS
    if unknown_suite:
        raise ProfileValidationError(
            f"Profile '{name}' contains unknown suite keys: "
            f"{', '.join(sorted(unknown_suite))}"
        )

    unknown_identify = set(identify.keys()) - VALID_IDENTIFY_KEYS
    if unknown_identify:
        raise ProfileValidationError(
            f"Profile '{name}' contains unknown identify keys: "
            f"{', '.join(sorted(unknown_identify))}"
        )


def _get_bundled_profile_path(name: str) -> Optional[Path]:
    """Get path to bundled profile using importlib.resources."""
    if HAS_IMPORTLIB_RESOURCES:
        try:
            profiles_pkg = importlib_files('specimux_suite.profiles')
            profile_file = profiles_pkg.joinpath(f'{name}.yaml')
            with as_file(profile_file) as path:
                if path.exists():
                    return path
        except (TypeError, FileNotFoundError, ModuleNotFoundError):
            pass

    # Last resort: check relative to this file's directory (profiles/__init__.py)
    bundled_dir = Path(__file__).parent
    bundled_path = bundled_dir / f'{name}.yaml'
    if bundled_path.exists():
        return bundled_path

    return None


def _list_bundled_profiles() -> list[str]:
    """List names of bundled profiles."""
    profiles = []

    if HAS_IMPORTLIB_RESOURCES:
        try:
            profiles_pkg = importlib_files('specimux_suite.profiles')
            for item in profiles_pkg.iterdir():
                if str(item).endswith('.yaml'):
                    name = Path(str(item)).stem
                    profiles.append(name)
            if profiles:
                return sorted(profiles)
        except (TypeError, FileNotFoundError, ModuleNotFoundError):
            pass

    # Fall back to checking directory relative to this file
    bundled_dir = Path(__file__).parent
    if bundled_dir.exists():
        for yaml_file in bundled_dir.glob('*.yaml'):
            profiles.append(yaml_file.stem)

    return sorted(profiles)


def list_profiles() -> list[str]:
    """List available profiles (user + bundled).

    Returns list of profile names (without .yaml extension).
    User profiles take precedence over bundled profiles with same name.
    """
    profiles: set[str] = set()

    # User profiles
    if PROFILES_DIR.exists():
        for yaml_file in PROFILES_DIR.glob('*.yaml'):
            profiles.add(yaml_file.stem)

    # Bundled profiles
    profiles.update(_list_bundled_profiles())

    return sorted(profiles)


def print_profiles_list() -> None:
    """Print available profiles to stdout."""
    if yaml is None:
        print("Profile support requires PyYAML. Install with: pip install pyyaml")
        return

    profiles = list_profiles()

    if not profiles:
        print("No profiles found.")
        print(f"\nProfiles are stored in: {PROFILES_DIR}")
        return

    print("Available profiles:\n")

    for name in profiles:
        try:
            profile = SuiteProfile.load(name, check_version=False)

            user_path = PROFILES_DIR / f"{name}.yaml"
            source = "user" if user_path.exists() else "bundled"

            compatible = _check_version_compatible(profile.version, __version__)
            compat_str = "" if compatible else " [INCOMPATIBLE]"

            print(f"  {name} ({source}){compat_str}")
            if profile.description:
                print(f"    {profile.description}")
            print(f"    Version: {profile.version}")
            print()

        except ProfileError as e:
            print(f"  {name} [ERROR: {e}]")
            print()

    print(f"Usage: specimux-suite batch|live ... -p <profile>")
    print(f"Profile directory: {PROFILES_DIR}")
