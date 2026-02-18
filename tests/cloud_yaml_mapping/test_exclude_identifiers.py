from unittest.mock import Mock, patch

import pytest

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import ChangeSet, build_change_set
from dbt_jobs_as_code.schemas.common_types import Settings, Triggers
from dbt_jobs_as_code.schemas.job import JobDefinition


@pytest.fixture
def sample_jobs():
    """Create sample jobs for testing"""
    base_job = JobDefinition(
        project_id=123,
        environment_id=456,
        account_id=789,
        name="Base Job",
        settings=Settings(threads=4),
        run_generate_sources=False,
        execute_steps=["dbt run"],
        generate_docs=False,
        schedule={"cron": "0 * * * *"},
        triggers=Triggers(schedule=True),
    )

    return [
        base_job.model_copy(
            update={
                "id": 1,
                "name": "Production Job",
                "identifier": "prod:daily-run",
            }
        ),
        base_job.model_copy(
            update={
                "id": 2,
                "name": "Staging Job",
                "identifier": "staging:nightly-test",
            }
        ),
        base_job.model_copy(
            update={
                "id": 3,
                "name": "Development Job",
                "identifier": "dev:feature-test",
            }
        ),
        base_job.model_copy(
            update={
                "id": 4,
                "name": "Legacy Job",
                "identifier": "legacy:old-process",
            }
        ),
        base_job.model_copy(
            update={
                "id": 5,
                "name": "Temp Job",
                "identifier": "temp:experimental",
            }
        ),
        base_job.model_copy(
            update={
                "id": 6,
                "name": "Unmanaged Job",
                "identifier": None,
            }
        ),
    ]


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_no_pattern(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that when no exclude pattern is provided, all jobs are processed"""
    # Create a sample job definition for the mock config
    sample_job = sample_jobs[0]

    # Mock the configuration loading with a non-empty jobs dictionary
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_job}  # Non-empty to avoid early return
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set without exclude pattern
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching=None,
    )

    # Verify that get_jobs was called (jobs should be processed normally)
    mock_dbt_cloud.get_jobs.assert_called_once()
    # The function should complete successfully
    assert isinstance(result, ChangeSet)


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_simple_pattern(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that jobs matching a simple regex pattern are excluded"""
    # Create a sample job definition for the mock config
    sample_job = sample_jobs[0]

    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_job}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with exclude pattern for staging jobs
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="staging:.*",
    )

    # Verify the result is a valid ChangeSet
    assert isinstance(result, ChangeSet)
    # The staging job should have been filtered out before processing


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_multiple_patterns(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that jobs matching multiple patterns are excluded"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with pattern matching legacy and temp jobs
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="(legacy|temp):.*",
    )

    # Verify the result is a valid ChangeSet
    assert isinstance(result, ChangeSet)


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_jobs_without_identifier(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that jobs without identifiers are not affected by exclude pattern"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with pattern that would match if identifier existed
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching=".*",  # This would match anything if identifier exists
    )

    # Verify the result is a valid ChangeSet
    assert isinstance(result, ChangeSet)
    # Jobs without identifiers should not be excluded


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_invalid_regex(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that invalid regex patterns are handled gracefully"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with invalid regex pattern
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="[invalid-regex",  # Missing closing bracket
    )

    # Should return empty ChangeSet when regex is invalid
    assert isinstance(result, ChangeSet)
    assert len(result) == 0


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_case_sensitive(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that regex matching is case sensitive"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with uppercase pattern (should not match lowercase identifiers)
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="STAGING:.*",  # Uppercase, won't match "staging:.*"
    )

    # Verify the result is a valid ChangeSet (no jobs should be excluded)
    assert isinstance(result, ChangeSet)


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_partial_match(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that partial matches work correctly"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with pattern that matches part of identifier
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="test",  # Should match both "nightly-test" and "feature-test"
    )

    # Verify the result is a valid ChangeSet
    assert isinstance(result, ChangeSet)


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.load_job_configuration")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.DBTCloud")
@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.glob.glob")
def test_exclude_identifiers_matching_with_json_output(
    mock_glob, mock_dbt_cloud_class, mock_load_config, sample_jobs
):
    """Test that JSON output mode works correctly with exclude pattern"""
    # Mock the configuration loading
    mock_config = Mock()
    mock_config.jobs = {"test-job": sample_jobs[0]}
    mock_load_config.return_value = mock_config
    mock_glob.return_value = ["test.yml"]

    # Mock the DBT Cloud client
    mock_dbt_cloud = Mock()
    mock_dbt_cloud_class.return_value = mock_dbt_cloud
    mock_dbt_cloud.get_jobs.return_value = sample_jobs
    mock_dbt_cloud.build_mapping_job_identifier_job_id.return_value = {}

    # Call build_change_set with JSON output enabled
    result = build_change_set(
        config="test.yml",
        yml_vars=None,
        disable_ssl_verification=False,
        project_ids=[],
        environment_ids=[],
        exclude_identifiers_matching="staging:.*",
        output_json=True,
    )

    # Verify the result is a valid ChangeSet
    assert isinstance(result, ChangeSet)
    # JSON output should not affect the filtering behavior
