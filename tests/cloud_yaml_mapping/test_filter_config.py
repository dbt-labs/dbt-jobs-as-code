import re
from io import StringIO

import pytest
from loguru import logger

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import filter_config


# Mock Job class for testing
class MockJob:
    def __init__(self, identifier, project_id, environment_id):
        self.identifier = identifier
        self.project_id = project_id
        self.environment_id = environment_id


@pytest.fixture
def defined_jobs():
    return {
        1: MockJob(identifier="job1", project_id=101, environment_id=201),
        2: MockJob(identifier="job2", project_id=102, environment_id=202),
        3: MockJob(identifier="job3", project_id=103, environment_id=201),
        4: MockJob(identifier="job4", project_id=104, environment_id=203),
    }


@pytest.fixture
def log_capture():
    log_stream = StringIO()
    logger.add(log_stream, format="{message}")
    yield log_stream
    logger.remove()


def test_filter_config_no_filters(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[], environment_ids=[])
    assert len(result) == 4
    assert result == defined_jobs
    log_output = log_capture.getvalue().strip()
    assert log_output == ""


def test_filter_config_with_project_ids(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[101, 103], environment_ids=[])
    assert len(result) == 2
    assert 1 in result
    assert 3 in result
    log_output = log_capture.getvalue().strip()
    assert re.search(r"job2.*project_id", log_output)
    assert re.search(r"job4.*project_id", log_output)


def test_filter_config_with_environment_ids(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[], environment_ids=[201])
    assert len(result) == 2
    assert 1 in result
    assert 3 in result
    log_output = log_capture.getvalue().strip()
    assert re.search(r"job2.*environment_id", log_output)
    assert re.search(r"job4.*environment_id", log_output)


def test_filter_config_with_project_and_environment_ids(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[101, 103], environment_ids=[201])
    assert len(result) == 2
    assert 1 in result
    assert 3 in result
    log_output = log_capture.getvalue().strip()
    assert re.search(r"job2.*environment_id", log_output)
    assert re.search(r"job2.*project_id", log_output)
    assert re.search(r"job4.*environment_id", log_output)
    assert re.search(r"job4.*project_id", log_output)


def test_filter_config_no_matching_project_ids(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[999], environment_ids=[])
    assert len(result) == 0
    log_output = log_capture.getvalue().strip()
    assert re.search(r"job1.*project_id", log_output)
    assert re.search(r"job2.*project_id", log_output)
    assert re.search(r"job3.*project_id", log_output)
    assert re.search(r"job4.*project_id", log_output)


def test_filter_config_no_matching_environment_ids(defined_jobs, log_capture):
    result = filter_config(defined_jobs, project_ids=[], environment_ids=[999])
    assert len(result) == 0
    log_output = log_capture.getvalue().strip()
    assert re.search(r"job1.*environment_id", log_output)
    assert re.search(r"job2.*environment_id", log_output)
    assert re.search(r"job3.*environment_id", log_output)
    assert re.search(r"job4.*environment_id", log_output)
