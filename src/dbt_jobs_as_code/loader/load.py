import glob

import yaml
from beartype.typing import List, Optional, Set
from jinja2 import Environment, StrictUndefined, meta
from jinja2.exceptions import UndefinedError
from loguru import logger

from dbt_jobs_as_code.schemas.config import Config


class LoadingJobsYAMLError(Exception):
    pass


def load_job_configuration(config_files: List[str], vars_file: Optional[List[str]]) -> Config:
    """Load the job configuration set in a YAML file into a Config object

    Can be a non-templated YAML or a templated one for which we need to replace Jinja values
    """
    if vars_file:
        config = _load_yaml_with_template(config_files, vars_file)
    else:
        config = _load_yaml_no_template(config_files)

    if config.get("jobs", {}) == {}:
        return Config(jobs={})

    date_config = [job.get("schedule", {}).get("date", None) for job in config["jobs"].values()]
    time_config = [job.get("schedule", {}).get("time", None) for job in config["jobs"].values()]

    if any(date_config):
        logger.warning(
            "⚡️ There is some date config under 'schedule > date' in your YML. This data is auto generated and should be deleted. Only cron is supported in the config."
        )
    if any(time_config):
        logger.warning(
            "⚡️ There is some time config under 'schedule > time' in your YML. This data is auto generated and should be deleted. Only cron is supported in the config."
        )

    for identifier, job in config.get("jobs", {}).items():
        job["identifier"] = identifier

    return Config(**config)


def _load_yaml_no_template(config_files: List[str]) -> dict:
    """Load a job YAML file into a Config object"""

    combined_config = {}
    for config_file in config_files:
        with open(config_file) as f:
            config_string = f.read()

            jinja_vars = _get_jinja_variables(config_string)
            if jinja_vars:
                raise LoadingJobsYAMLError(
                    f"{config_file} is a templated YAML file. Please remove the variables {jinja_vars} or provide the variables values."
                )

            config = yaml.safe_load(config_string)
            if config:
                # Merge the jobs from each file into combined_config
                if config.get("jobs", {}) != {}:
                    if "jobs" not in combined_config:
                        combined_config["jobs"] = {}
                    combined_config["jobs"].update(config["jobs"])
                # Merge any other top-level keys
                for key, value in config.items():
                    if key != "jobs":
                        combined_config[key] = value

    return combined_config


def _replace_none_with_null(obj):
    if isinstance(obj, dict):
        return {k: _replace_none_with_null(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_replace_none_with_null(item) for item in obj]
    return "null" if obj is None else obj


def _load_vars_files(vars_file: List[str]) -> dict:
    """Load and merge multiple vars files into a single dictionary.

    Args:
        vars_file: List of paths to vars files

    Returns:
        Dictionary containing merged variable values

    Raises:
        LoadingJobsYAMLError: If duplicate variables are found across files
    """
    template_vars_values = {}
    for vars_path in vars_file:
        with open(vars_path) as f:
            # we load the vars. if there is no data in them we set it to an empty dict
            vars_data = yaml.safe_load(f) or {}
            # Check for duplicate variables
            for key in vars_data:
                if key in template_vars_values:
                    raise LoadingJobsYAMLError(
                        f"Variable '{key}' is defined multiple times in vars files"
                    )
            template_vars_values.update(vars_data)
    return _replace_none_with_null(template_vars_values)  # type: ignore


def _load_yaml_with_template(config_files: List[str], vars_file: List[str]) -> dict:
    """Load a job YAML file into a Config object"""
    # Load and merge vars files
    template_vars_values = _load_vars_files(vars_file)

    # Load and combine config files
    combined_config = {}
    env = Environment(undefined=StrictUndefined)

    for config_path in config_files:
        with open(config_path) as f:
            config_string_unrendered = f.read()
            template = env.from_string(config_string_unrendered)

            try:
                config_string_rendered = template.render(template_vars_values)
            except UndefinedError as e:
                raise LoadingJobsYAMLError(
                    f"Some variables didn't have a value: {e.message}."
                ) from e

            config = yaml.safe_load(config_string_rendered)
            if config:
                # Merge the jobs from each file
                if "jobs" in config:
                    if "jobs" not in combined_config:
                        combined_config["jobs"] = {}
                    combined_config["jobs"].update(config["jobs"])
                # Merge any other top-level keys
                for key, value in config.items():
                    if key != "jobs":
                        combined_config[key] = value

    return combined_config


def _get_jinja_variables(input: str) -> Set[str]:
    """Get the variables from a Jinja template"""
    env = Environment()
    parsed_input = env.parse(input)
    return meta.find_undeclared_variables(parsed_input)


def resolve_file_paths(
    config_pattern: Optional[str], vars_pattern: Optional[str] = None
) -> tuple[List[str], List[str]]:
    """
    Resolve glob patterns to lists of file paths.

    Args:
        config_pattern: Glob pattern for config files
        vars_pattern: Optional glob pattern for vars files

    Returns:
        Tuple of (config_files, vars_files)

    Raises:
        LoadingJobsYAMLError: If no files match a pattern when pattern is provided
    """
    if not config_pattern:
        return [], []

    config_files = glob.glob(config_pattern)
    if not config_files:
        raise LoadingJobsYAMLError(f"No files found matching pattern: {config_pattern}")

    vars_files = []
    if vars_pattern:
        vars_files = glob.glob(vars_pattern)
        if not vars_files:
            raise LoadingJobsYAMLError(f"No files found matching pattern: {vars_pattern}")

    return config_files, vars_files
