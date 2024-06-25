import yaml
from beartype.typing import Optional, Set, TextIO
from jinja2 import Environment, StrictUndefined, meta
from jinja2.exceptions import UndefinedError
from loguru import logger

from src.schemas.config import Config


class LoadingJobsYAMLError(Exception):
    pass


def load_job_configuration(config_file: TextIO, vars_file: Optional[TextIO]) -> Config:
    """Load the job configuration set in a YAML file into a Config object

    Can be a non-templated YAML or a templated one for which we need to replace Jinja values
    """
    if vars_file:
        config = _load_yaml_with_template(config_file, vars_file)
    else:
        config = _load_yaml_no_template(config_file)

    if not config["jobs"]:
        return Config(jobs={})

    date_config = [job.get("schedule", {}).get("date", None) for job in config["jobs"].values()]
    time_config = [job.get("schedule", {}).get("time", None) for job in config["jobs"].values()]

    if any(date_config):
        logger.warning(
            f"⚡️ There is some date config under 'schedule > date' in your YML. This data is auto generated and should be deleted. Only cron is supported in the config."
        )
    if any(time_config):
        logger.warning(
            f"⚡️ There is some time config under 'schedule > time' in your YML. This data is auto generated and should be deleted. Only cron is supported in the config."
        )

    for identifier, job in config.get("jobs", {}).items():
        job["identifier"] = identifier

    return Config(**config)


def _load_yaml_no_template(config_file: TextIO) -> dict:
    """Load a job YAML file into a Config object"""
    config_string = config_file.read()

    jinja_vars = _get_jinja_variables(config_string)
    if jinja_vars:
        raise LoadingJobsYAMLError(
            f"This is a templated YAML file. Please remove the variables {jinja_vars} or provide the variables values."
        )

    return yaml.safe_load(config_string)


def _load_yaml_with_template(config_file: TextIO, vars_file: TextIO) -> dict:
    """Load a job YAML file into a Config object"""
    template_vars_values = yaml.safe_load(vars_file)
    config_string_unrendered = config_file.read()

    env = Environment(undefined=StrictUndefined)
    template = env.from_string(config_string_unrendered)

    try:
        config_string_rendered = template.render(template_vars_values)
    except UndefinedError as e:
        print(f"Error: {e}")  # This will raise an error
        raise LoadingJobsYAMLError(f"Some variables didn't have a value: {e.message}.")

    return yaml.safe_load(config_string_rendered)


def _get_jinja_variables(input: str) -> Set[str]:
    """Get the variables from a Jinja template"""
    env = Environment()
    parsed_input = env.parse(input)
    return meta.find_undeclared_variables(parsed_input)
