import yaml
from loguru import logger

from src.schemas.config import Config


def load_job_configuration(config_file) -> Config:
    """Load a job YAML file into a Config object"""
    config = yaml.safe_load(config_file)

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

    for identifier, job in config.get("jobs").items():
        job["identifier"] = identifier

    return Config(**config)
