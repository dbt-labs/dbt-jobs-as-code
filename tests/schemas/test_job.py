import json

import pytest
from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema import validate
from pydantic import ValidationError

from dbt_jobs_as_code.schemas.config import generate_config_schema
from dbt_jobs_as_code.schemas.job import (
    IdentifierInfo,
    JobDefinition,
    filter_jobs_by_import_filter,
)


@pytest.fixture
def test_job_factory():
    """Fixture providing a factory function to create test jobs."""

    def _create_job(name: str, filter_import: str = "") -> JobDefinition:
        # If filter_import is provided, add it to the name in the correct format
        job_name = name
        if filter_import:
            if ":" in filter_import:
                job_name = f"{name} [[{filter_import}]]"
            else:
                job_name = f"{name} [[{filter_import}:test-id]]"

        return JobDefinition(
            name=job_name,
            account_id=1,
            project_id=1,
            environment_id=1,
            settings={},
            schedule={"cron": "0 0 * * *"},
            triggers={},
            execute_steps=[],
            run_generate_sources=False,
            generate_docs=False,
        )

    return _create_job


@pytest.fixture
def mixed_filter_jobs(test_job_factory):
    """Fixture providing a list of jobs with different filter configurations."""
    return [
        test_job_factory("Job 1", "prod"),  # will become [[prod:test-id]]
        test_job_factory("Job 2", "dev"),  # will become [[dev:test-id]]
        test_job_factory("Job 3", "prod,staging:test-id"),  # will become [[prod,staging:test-id]]
        test_job_factory("Job 4"),  # empty filter
        test_job_factory("Job 5", "*"),  # will become [[*:test-id]]
    ]


class TestIdentifierExtraction:
    """Tests for the _extract_identifier_from_name function."""

    def test_no_identifier(self):
        """Test when job name has no identifier."""
        name = "My Job"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")

    def test_simple_identifier(self):
        """Test when job name has a simple identifier."""
        name = "My Job [[test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="", raw_identifier="test-job"
        )

    def test_identifier_with_filter(self):
        """Test when job name has an identifier with import filter."""
        name = "My Job [[prod:test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="prod", raw_identifier="prod:test-job"
        )

    def test_identifier_with_wildcard_filter(self):
        """Test when job name has an identifier with wildcard filter."""
        name = "My Job [[*:test-job]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="test-job", import_filter="*", raw_identifier="*:test-job"
        )

    def test_complex_identifier(self):
        """Test when job name has a complex identifier with allowed special characters."""
        name = "My Job [[env:my-complex_job-123]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(
            identifier="my-complex_job-123",
            import_filter="env",
            raw_identifier="env:my-complex_job-123",
        )

    def test_invalid_format(self):
        """Test when job name has an invalid identifier format with multiple colons."""
        name = "My Job [[env:test:invalid]]"
        with pytest.raises(ValueError) as exc_info:
            JobDefinition._extract_identifier_from_name(name)
        assert (
            str(exc_info.value) == "Invalid job identifier - More than 1 colon: 'env:test:invalid'"
        )

    def test_empty_identifier(self):
        """Test when job name has empty identifier brackets."""
        name = "My Job [[]]"
        result = JobDefinition._extract_identifier_from_name(name)
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")


class TestJobFiltering:
    """Tests for the filter_jobs_by_import_filter function."""

    def test_no_filter(self, mixed_filter_jobs):
        """Test when no filter is provided, all jobs should be returned."""
        result = filter_jobs_by_import_filter(mixed_filter_jobs, None)
        assert len(result) == 5
        assert all(job in result for job in mixed_filter_jobs)

        result = filter_jobs_by_import_filter(mixed_filter_jobs, "")
        assert len(result) == 5
        assert all(job in result for job in mixed_filter_jobs)

    def test_with_filter(self, mixed_filter_jobs):
        """Test filtering jobs with specific filter value."""
        result = filter_jobs_by_import_filter(mixed_filter_jobs, "prod")
        assert len(result) == 4
        assert mixed_filter_jobs[0] in result  # matches 'prod'
        assert mixed_filter_jobs[2] in result  # matches 'prod,staging'
        assert mixed_filter_jobs[3] in result  # empty filter
        assert mixed_filter_jobs[4] in result  # wildcard filter
        assert mixed_filter_jobs[1] not in result  # 'dev' doesn't match

    def test_wildcard_filter(self, test_job_factory):
        """Test that jobs with wildcard filter are always included."""
        jobs = [
            test_job_factory("Job 1", "*"),
            test_job_factory("Job 2", "prod"),
        ]

        result = filter_jobs_by_import_filter(jobs, "any-filter")
        assert len(result) == 1
        assert jobs[0] in result
        assert jobs[1] not in result

    def test_empty_filter(self, test_job_factory):
        """Test that jobs with empty filter are always included."""
        jobs = [
            test_job_factory("Job 1"),  # empty filter
            test_job_factory("Job 2", "prod"),
        ]

        result = filter_jobs_by_import_filter(jobs, "any-filter")
        assert len(result) == 1
        assert jobs[0] in result
        assert jobs[1] not in result


BASE_JOB_DATA = {
    "name": "Test Job",
    "account_id": 1,
    "project_id": 1,
    "environment_id": 1,
    "settings": {},
    "triggers": {},
    "execute_steps": ["dbt build"],
    "run_generate_sources": False,
    "generate_docs": False,
}


class TestScheduleConditionalRequirement:
    """Tests for schedule being optional on ci/merge jobs."""

    # -- Pydantic model tests --

    @pytest.mark.parametrize("job_type", ["ci", "merge"])
    def test_pydantic_schedule_optional_for_ci_merge(self, job_type):
        job = JobDefinition(**{**BASE_JOB_DATA, "job_type": job_type})
        assert job.schedule is not None  # defaults to Schedule()

    @pytest.mark.parametrize("job_type", ["scheduled", "other"])
    def test_pydantic_schedule_required_for_scheduled_other(self, job_type):
        with pytest.raises(ValidationError, match="schedule"):
            JobDefinition(**{**BASE_JOB_DATA, "job_type": job_type})

    def test_pydantic_schedule_required_when_job_type_absent(self):
        with pytest.raises(ValidationError, match="schedule"):
            JobDefinition(**BASE_JOB_DATA)  # job_type defaults to "scheduled"

    # -- JSON schema tests --

    @pytest.fixture
    def json_schema(self):
        return json.loads(generate_config_schema())

    def _config_instance(self, job_type=None, include_schedule=False):
        """Build a minimal config dict for JSON schema validation."""
        job = {
            "name": "Test Job",
            "account_id": 1,
            "project_id": 1,
            "environment_id": 1,
            "settings": {},
            "triggers": {},
            "execute_steps": ["dbt build"],
            "run_generate_sources": False,
            "generate_docs": False,
        }
        if job_type is not None:
            job["job_type"] = job_type
        if include_schedule:
            job["schedule"] = {"cron": "0 0 * * *"}
        return {"jobs": {"test_job": job}}

    @pytest.mark.parametrize("job_type", ["ci", "merge"])
    def test_json_schema_schedule_optional_for_ci_merge(self, json_schema, job_type):
        instance = self._config_instance(job_type=job_type, include_schedule=False)
        validate(instance=instance, schema=json_schema)

    @pytest.mark.parametrize("job_type", ["scheduled", "other"])
    def test_json_schema_schedule_required_for_scheduled_other(self, json_schema, job_type):
        instance = self._config_instance(job_type=job_type, include_schedule=False)
        with pytest.raises(JsonSchemaValidationError, match="schedule"):
            validate(instance=instance, schema=json_schema)

    def test_json_schema_schedule_required_when_job_type_absent(self, json_schema):
        instance = self._config_instance(include_schedule=False)
        with pytest.raises(JsonSchemaValidationError, match="schedule"):
            validate(instance=instance, schema=json_schema)

    @pytest.mark.parametrize("job_type", ["scheduled", "ci", "merge", "other"])
    def test_json_schema_schedule_accepted_for_all_types(self, json_schema, job_type):
        """Providing schedule is always valid regardless of job_type."""
        instance = self._config_instance(job_type=job_type, include_schedule=True)
        validate(instance=instance, schema=json_schema)
