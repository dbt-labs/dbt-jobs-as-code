import yaml

from schemas.config import Config


def load_job_configuration(path) -> Config:
    """Load a job YAML file into a Config object"""
    with open(path, "r") as config_file:
        config = yaml.safe_load(config_file)

    return Config(**config)

