import copy
from unittest.mock import Mock

import pytest

from dbt_jobs_as_code.client import DBTCloud, DBTCloudException
from dbt_jobs_as_code.cloud_yaml_mapping.validate_link import can_be_linked
from dbt_jobs_as_code.schemas.job import JobDefinition


@pytest.fixture
def mock_dbt_cloud():
    return Mock(spec=DBTCloud)


@pytest.fixture
def base_job_definition():
    return JobDefinition(
        id=None,
        linked_id=123,
        identifier=None,
        account_id=1,
        project_id=1,
        environment_id=1,
        name="Test Job",
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )


def test_cant_be_linked_no_id_in_yaml(mock_dbt_cloud, base_job_definition):
    base_job_definition.linked_id = None

    result = can_be_linked("test_job", base_job_definition, mock_dbt_cloud)

    assert result.can_be_linked is False
    assert "doesn't have an ID in YAML" in result.message
    assert result.linked_job is None


def test_cant_be_linked_job_not_exist(mock_dbt_cloud, base_job_definition):
    mock_dbt_cloud.get_job.side_effect = DBTCloudException("Job not found")

    result = can_be_linked("test_job", base_job_definition, mock_dbt_cloud)

    assert result.can_be_linked is False
    assert "doesn't exist in dbt Cloud" in result.message
    assert result.linked_job is None
    mock_dbt_cloud.get_job.assert_called_once_with(job_id=123)


def test_cant_be_linked_already_linked(mock_dbt_cloud, base_job_definition):
    cloud_job = copy.deepcopy(base_job_definition)
    cloud_job.identifier = "existing_identifier"

    mock_dbt_cloud.get_job.return_value = cloud_job

    result = can_be_linked("test_job", base_job_definition, mock_dbt_cloud)

    assert result.can_be_linked is False
    assert "already linked" in result.message
    assert result.linked_job is None
    mock_dbt_cloud.get_job.assert_called_once_with(job_id=123)


def test_can_be_linked_success(mock_dbt_cloud, base_job_definition):
    cloud_job = copy.deepcopy(base_job_definition)
    cloud_job.id = 123
    mock_dbt_cloud.get_job.return_value = cloud_job

    result = can_be_linked("test_job", base_job_definition, mock_dbt_cloud)

    assert result.can_be_linked is True
    assert result.message == ""
    assert result.linked_job == cloud_job
    mock_dbt_cloud.get_job.assert_called_once_with(job_id=123)
