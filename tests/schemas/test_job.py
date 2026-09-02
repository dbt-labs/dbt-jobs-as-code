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

    def test_extract_identifier_from_description_simple(self):
        """Test extracting simple identifier from job description."""
        result = JobDefinition._extract_identifier_from_description("Runs nightly [[daily_job]]")
        assert result == IdentifierInfo(
            identifier="daily_job", import_filter="", raw_identifier="daily_job"
        )

    def test_extract_identifier_from_description_with_filter(self):
        """Test extracting identifier with filter from job description."""
        result = JobDefinition._extract_identifier_from_description(
            "Runs nightly [[prod:daily_job]]"
        )
        assert result == IdentifierInfo(
            identifier="daily_job", import_filter="prod", raw_identifier="prod:daily_job"
        )

    def test_extract_identifier_from_description_no_identifier(self):
        """Test when description has no identifier."""
        result = JobDefinition._extract_identifier_from_description("Runs nightly")
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")

    def test_extract_identifier_from_description_empty(self):
        """Test when description is empty."""
        result = JobDefinition._extract_identifier_from_description("")
        assert result == IdentifierInfo(identifier=None, import_filter="", raw_identifier="")

    def test_extract_identifier_from_description_only_tag(self):
        """Test when description contains only the identifier tag."""
        result = JobDefinition._extract_identifier_from_description("[[daily_job]]")
        assert result == IdentifierInfo(
            identifier="daily_job", import_filter="", raw_identifier="daily_job"
        )


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


class TestCostOptimizationFeatures:
    """Tests for cost_optimization_features field on JobDefinition."""

    def test_pydantic_defaults_to_empty_list(self):
        job = JobDefinition(**{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}})
        assert job.cost_optimization_features == []

    def test_pydantic_accepts_valid_features(self):
        job = JobDefinition(
            **{
                **BASE_JOB_DATA,
                "schedule": {"cron": "0 0 * * *"},
                "cost_optimization_features": ["state_aware_orchestration"],
            }
        )
        assert job.cost_optimization_features == ["state_aware_orchestration"]

    def test_pydantic_accepts_multiple_features(self):
        job = JobDefinition(
            **{
                **BASE_JOB_DATA,
                "schedule": {"cron": "0 0 * * *"},
                "cost_optimization_features": [
                    "state_aware_orchestration",
                    "efficient_testing",
                ],
            }
        )
        assert job.cost_optimization_features == [
            "state_aware_orchestration",
            "efficient_testing",
        ]

    def test_pydantic_accepts_empty_list(self):
        job = JobDefinition(
            **{
                **BASE_JOB_DATA,
                "schedule": {"cron": "0 0 * * *"},
                "cost_optimization_features": [],
            }
        )
        assert job.cost_optimization_features == []

    def test_payload_includes_cost_optimization_features(self):
        job = JobDefinition(
            **{
                **BASE_JOB_DATA,
                "schedule": {"cron": "0 0 * * *"},
                "cost_optimization_features": ["state_aware_orchestration"],
            }
        )
        payload = json.loads(job.to_payload())
        assert payload["cost_optimization_features"] == ["state_aware_orchestration"]

    def test_payload_includes_empty_list_when_not_set(self):
        job = JobDefinition(**{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}})
        payload = json.loads(job.to_payload())
        assert payload["cost_optimization_features"] == []

    @pytest.fixture
    def json_schema(self):
        return json.loads(generate_config_schema())

    def test_json_schema_accepts_valid_features(self, json_schema):
        instance = {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    "cost_optimization_features": ["state_aware_orchestration"],
                }
            }
        }
        validate(instance=instance, schema=json_schema)

    def test_json_schema_rejects_invalid_feature(self, json_schema):
        instance = {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    "cost_optimization_features": ["not_a_real_feature"],
                }
            }
        }
        with pytest.raises(JsonSchemaValidationError):
            validate(instance=instance, schema=json_schema)


class TestDescriptionValidation:
    """Tests for description field length validation."""

    def test_description_within_limit_is_accepted(self):
        job = JobDefinition(
            **{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}, "description": "x" * 255}
        )
        assert len(job.description) == 255

    def test_description_exceeding_limit_raises_validation_error(self):
        with pytest.raises(ValidationError, match="description"):
            JobDefinition(
                **{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}, "description": "x" * 256}
            )

    def test_empty_description_is_accepted(self):
        job = JobDefinition(**{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}})
        assert job.description == ""

    @pytest.fixture
    def json_schema(self):
        return json.loads(generate_config_schema())

    def test_json_schema_rejects_description_exceeding_limit(self, json_schema):
        instance = {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    "description": "x" * 256,
                }
            }
        }
        with pytest.raises(JsonSchemaValidationError, match="description"):
            validate(instance=instance, schema=json_schema)


class TestToPayloadDescMode:
    """Tests for to_payload() with use_desc_for_id=True."""

    def _make_job(self, name="Test Job", description="", identifier=None):
        job = JobDefinition(
            **{
                **BASE_JOB_DATA,
                "schedule": {"cron": "0 0 * * *"},
                "name": f"{name} [[{identifier}]]" if identifier else name,
                "description": description,
            }
        )
        return job

    def test_to_payload_use_desc_for_id(self):
        """Identifier goes to description, name is clean."""
        job = self._make_job(description="Runs nightly", identifier="daily_job")
        payload = json.loads(job.to_payload(use_desc_for_id=True))
        assert payload["name"] == "Test Job"
        assert payload["description"] == "Runs nightly [[daily_job]]"

    def test_to_payload_use_desc_for_id_empty_description(self):
        """Empty description stores [[id]] without leading space."""
        job = self._make_job(description="", identifier="daily_job")
        payload = json.loads(job.to_payload(use_desc_for_id=True))
        assert payload["name"] == "Test Job"
        assert payload["description"] == "[[daily_job]]"

    def test_to_payload_use_desc_for_id_no_identifier(self):
        """No identifier: both fields remain clean."""
        job = self._make_job(description="Runs nightly")
        payload = json.loads(job.to_payload(use_desc_for_id=True))
        assert payload["name"] == "Test Job"
        assert payload["description"] == "Runs nightly"

    def test_to_payload_default_mode_unchanged(self):
        """use_desc_for_id=False (default): identifier still goes in name."""
        job = self._make_job(description="Runs nightly", identifier="daily_job")
        payload = json.loads(job.to_payload())
        assert payload["name"] == "Test Job [[daily_job]]"
        assert payload["description"] == "Runs nightly"

    def test_to_payload_description_at_limit(self):
        """Description + [[identifier]] at exactly 255 chars is accepted."""
        # "x" * 240 + " [[daily_job]]" = 240 + 14 = 254 chars — within limit
        long_desc = "x" * 240
        job = self._make_job(description=long_desc, identifier="daily_job")
        payload = json.loads(job.to_payload(use_desc_for_id=True))
        assert len(payload["description"]) == 254

    def test_to_payload_description_over_limit(self):
        """ValueError when description + [[identifier]] exceeds 255 chars."""
        # "x" * 242 + " [[daily_job]]" = 242 + 14 = 256 chars — over limit
        too_long_desc = "x" * 242
        job = self._make_job(description=too_long_desc, identifier="daily_job")
        with pytest.raises(ValueError, match="description"):
            job.to_payload(use_desc_for_id=True)

    def test_to_payload_description_barely_over_with_long_base(self):
        """ValueError when a nearly-full base description pushes the stored string over 255."""
        # "x" * 250 + " [[id]]" = 257 chars — should fail
        job = self._make_job(description="x" * 250, identifier="id")
        with pytest.raises(ValueError, match="description"):
            job.to_payload(use_desc_for_id=True)


class TestSelfDeferring:
    """Tests for the self_deferring field ("This Job" in the dbt Cloud UI)."""

    def _make_job(self, **overrides):
        return JobDefinition(**{**BASE_JOB_DATA, "schedule": {"cron": "0 0 * * *"}, **overrides})

    def test_defaults_to_false(self):
        job = self._make_job()
        assert job.self_deferring is False

    def test_mutually_exclusive_with_deferring_job_definition_id(self):
        with pytest.raises(ValidationError, match="self_deferring"):
            self._make_job(self_deferring=True, deferring_job_definition_id=42)

    @pytest.fixture
    def json_schema_for_exclusivity(self):
        return json.loads(generate_config_schema())

    def _config_instance(self, **job_overrides):
        return {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    **job_overrides,
                }
            }
        }

    def test_json_schema_rejects_self_deferring_with_deferring_job_definition_id(
        self, json_schema_for_exclusivity
    ):
        """The mutual-exclusion rule enforced by the Pydantic validator should also
        be catchable by JSON schema tooling alone (IDE YAML validation, `jsonschema`),
        without running Python."""
        instance = self._config_instance(self_deferring=True, deferring_job_definition_id=42)
        with pytest.raises(JsonSchemaValidationError):
            validate(instance=instance, schema=json_schema_for_exclusivity)

    def test_json_schema_accepts_self_deferring_alone(self, json_schema_for_exclusivity):
        instance = self._config_instance(self_deferring=True)
        validate(instance=instance, schema=json_schema_for_exclusivity)

    def test_json_schema_accepts_deferring_job_definition_id_alone(
        self, json_schema_for_exclusivity
    ):
        instance = self._config_instance(deferring_job_definition_id=42)
        validate(instance=instance, schema=json_schema_for_exclusivity)

    def test_to_payload_with_unknown_id_sends_null(self):
        """Brand-new job: id isn't known yet, so we can't self-reference on create."""
        job = self._make_job(self_deferring=True)
        payload = json.loads(job.to_payload())
        assert payload["deferring_job_definition_id"] is None

    def test_to_payload_with_known_id_self_references(self):
        """Existing job: id is known, so the payload points the job at itself."""
        job = self._make_job(self_deferring=True, id=42)
        payload = json.loads(job.to_payload())
        assert payload["deferring_job_definition_id"] == 42

    def test_to_payload_excludes_self_deferring_field(self):
        """self_deferring isn't a real dbt Cloud API field, only deferring_job_definition_id is."""
        job = self._make_job(self_deferring=True, id=42)
        payload = json.loads(job.to_payload())
        assert "self_deferring" not in payload

    @pytest.fixture
    def json_schema(self):
        return json.loads(generate_config_schema())

    def test_json_schema_accepts_self_deferring(self, json_schema):
        instance = {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    "self_deferring": True,
                }
            }
        }
        validate(instance=instance, schema=json_schema)

    def test_json_schema_rejects_non_boolean_self_deferring(self, json_schema):
        instance = {
            "jobs": {
                "test_job": {
                    **BASE_JOB_DATA,
                    "schedule": {"cron": "0 0 * * *"},
                    "self_deferring": "yes",
                }
            }
        }
        with pytest.raises(JsonSchemaValidationError):
            validate(instance=instance, schema=json_schema)

    def test_to_load_format_exports_legacy_self_id_as_self_deferring(self):
        """Importing a job that self-defers via a literal id (set before this flag
        existed, e.g. by hand in the dbt Cloud UI) should surface the new, portable
        self_deferring flag in the generated YAML, not the job's own numeric id --
        the whole point is a YAML file that isn't tied to one job's id."""
        job = self._make_job(id=42, deferring_job_definition_id=42)
        data = job.to_load_format()
        assert data["self_deferring"] is True
        assert data["deferring_job_definition_id"] is None

    def test_to_load_format_leaves_cross_job_deferring_untouched(self):
        """Deferring to a genuinely different job's id is unaffected."""
        job = self._make_job(id=42, deferring_job_definition_id=999)
        data = job.to_load_format()
        assert data["self_deferring"] is False
        assert data["deferring_job_definition_id"] == 999
