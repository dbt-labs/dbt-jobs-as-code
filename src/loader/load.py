import yaml

from schemas.config import Config


def load_job_configuration(config_file) -> Config:
    """Load a job YAML file into a Config object"""
    config = yaml.safe_load(config_file)

    for identifier, job in config.get("jobs").items():
        job["identifier"] = identifier

    return Config(**config)
