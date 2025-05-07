import re
from dataclasses import dataclass

from beartype.typing import Any, List, Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from dbt_jobs_as_code.schemas.common_types import (
    Execution,
    JobCompletionTriggerCondition,
    Schedule,
    Settings,
    Triggers,
    field_mandatory_int_allowed_as_string_in_schema,
    field_optional_int_allowed_as_string_in_schema,
)
from dbt_jobs_as_code.schemas.custom_environment_variable import CustomEnvironmentVariable


@dataclass
class IdentifierInfo:
    """Information extracted from a job identifier."""

    identifier: Optional[str]
    import_filter: Optional[str]
    raw_identifier: str


def filter_jobs_by_import_filter(
    jobs: List["JobDefinition"], filter_value: Optional[str]
) -> List["JobDefinition"]:
    """Filter jobs based on their identifier prefix.

    Args:
        jobs: List of jobs to filter
        filter_value: The filter value to match against. If None or empty, returns all jobs.

    Returns:
        List of jobs that match the filter criteria:
        - Jobs where filter_value is contained in _filter_import
        - Jobs with empty _filter_import
        - Jobs with _filter_import = "*"
    """
    if not filter_value:
        return jobs

    return [
        job
        for job in jobs
        if not job._filter_import  # empty filter
        or job._filter_import == "*"  # wildcard filter
        or filter_value in job._filter_import  # filter matches
    ]


# Main model for loader
class JobDefinition(BaseModel):
    """A definition for a dbt Cloud job."""

    linked_id: Optional[int] = Field(
        default=None,
        description="The ID of the job in dbt Cloud that we want to link. Only used for the 'link' command.",
    )
    id: Optional[int] = None
    identifier: Optional[str] = Field(
        default=None,
        description="The internal job identifier for the job for dbt-jobs-as-code. Will be added at the end of the job name.",
    )
    _filter_import: Optional[str] = None
    account_id: int = field_mandatory_int_allowed_as_string_in_schema
    project_id: int = field_mandatory_int_allowed_as_string_in_schema
    environment_id: int = field_mandatory_int_allowed_as_string_in_schema
    dbt_version: Optional[str] = None
    name: str
    settings: Settings
    execution: Execution = Execution()
    deferring_job_definition_id: Optional[int] = field_optional_int_allowed_as_string_in_schema
    deferring_environment_id: Optional[int] = field_optional_int_allowed_as_string_in_schema
    run_generate_sources: bool
    execute_steps: List[str]
    generate_docs: bool
    schedule: Schedule
    triggers: Triggers
    description: str = ""
    state: int = 1
    run_compare_changes: bool = False
    compare_changes_flags: str = "--select state:modified"
    # we don't want to enforce the list in case we add more, but still want to get those in the JSON schema
    job_type: str = Field(
        json_schema_extra={"enum": ["scheduled", "merge", "ci", "other"]},
        default="scheduled",
    )
    triggers_on_draft_pr: bool = False
    job_completion_trigger_condition: Optional[JobCompletionTriggerCondition] = None
    custom_environment_variables: List[CustomEnvironmentVariable] = Field(
        default=[],
        json_schema_extra={
            "items": {
                "type": "object",
                "patternProperties": {
                    "^DBT_": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "number"},
                            {"type": "boolean"},
                        ]
                    },
                },
                "additionalProperties": False,
            },
        },
        description="Dictionary of custom environment variables name and value for the job. The env var name must start with DBT_.",
    )

    def __init__(self, **data: Any):
        # Check if `name` includes an identifier. If yes, set the identifier in the object. Remove the identifier from
        # the name.
        identifier_info = self._extract_identifier_from_name(data["name"])
        _filter_import = identifier_info.import_filter
        if identifier_info.identifier:
            data["identifier"] = identifier_info.identifier
            data["name"] = data["name"].replace(f" [[{identifier_info.raw_identifier}]]", "")

        # Rewrite custom environment variables to include account and project id
        environment_variables = data.get("custom_environment_variables", None)
        if environment_variables:
            data["custom_environment_variables"] = [
                {
                    "name": list(variable.keys())[0],
                    "value": list(variable.values())[0],
                    "project_id": data["project_id"],
                    "account_id": data["account_id"],
                }
                for variable in environment_variables
            ]
        else:
            data["custom_environment_variables"] = []

        super().__init__(**data)
        self._filter_import = _filter_import

    @staticmethod
    def _extract_identifier_from_name(name: str) -> IdentifierInfo:
        """Extract identifier and import filter from job name.

        Args:
            name: Job name that may contain an identifier in [[identifier]] format

        Returns:
            IdentifierInfo containing identifier details

        Raises:
            ValueError: If identifier format is invalid
        """
        matches = re.search(r"\[\[([*:a-zA-Z0-9_-]+)\]\]", name)
        if matches is None:
            return IdentifierInfo(identifier=None, import_filter="", raw_identifier="")

        raw_identifier = matches.groups()[0]
        num_colons = raw_identifier.count(":")

        if num_colons == 0:
            return IdentifierInfo(
                identifier=raw_identifier, import_filter="", raw_identifier=raw_identifier
            )
        elif num_colons == 1:
            import_filter, identifier = raw_identifier.split(":")
            return IdentifierInfo(
                identifier=identifier, import_filter=import_filter, raw_identifier=raw_identifier
            )
        else:
            raise ValueError(f"Invalid job identifier - More than 1 colon: '{raw_identifier}'")

    def to_payload(self):
        """Create a dbt Cloud API payload for a JobDefinition."""

        # Rewrite the job name to embed the job ID from job.yml
        payload = self.model_copy()
        # if there is an identifier, add it to the name
        # otherwise, it means that we are "unlinking" the job from the job.yml
        if self.identifier:
            payload.name = f"{self.name} [[{self.identifier}]]"
        return payload.model_dump_json(
            exclude={"linked_id", "identifier", "custom_environment_variables"}
        )

    def to_load_format(self, include_linked_id: bool = False):
        """Generate a dict following our YML format to dump as YML later."""

        self.linked_id = self.id
        exclude_dict = {
            "identifier": True,
            "schedule": {
                "date": True,
                "time": True,
            },
            "id": True,
            "custom_environment_variables": True,
            "state": True,
        }
        if not include_linked_id:
            exclude_dict["linked_id"] = True

        data = self.model_dump(exclude=exclude_dict)
        data["custom_environment_variables"] = []
        for env_var in self.custom_environment_variables:
            data["custom_environment_variables"].append({env_var.name: env_var.value})
        return data

    def to_url(self, account_url: str) -> str:
        """Generate a URL for the job in dbt Cloud."""
        return f"{account_url}/deploy/{self.account_id}/projects/{self.project_id}/jobs/{self.id}"

    @model_validator(mode="after")
    def validate_cron_expression(self):
        """Validate the cron expression and include job ID in error message if invalid."""
        if not Schedule.validate_cron(self.schedule.cron):
            job_id_str = f" (Job ID: {self.id})" if self.id is not None else ""
            project_id_str = (
                f" (Project ID: {self.project_id})" if self.project_id is not None else ""
            )
            environment_id_str = (
                f" (Environment ID: {self.environment_id})"
                if self.environment_id is not None
                else ""
            )
            raise ValueError(
                f"The cron expression is not valid{job_id_str}{project_id_str}{environment_id_str}"
            )
        return self


class JobMissingFields(JobDefinition):
    """This class can be used to identify when there are new fields added to jobs
    We don't add the not needed fields to the JobDefinition model to prevent the tool from breaking with any API change
    """

    model_config = ConfigDict(extra="forbid")

    # when adding fields we also need to update the test for pytest

    # TODO: Add to JobDefinition model when the feature is out
    integration_id: Optional[int] = None
    run_lint: Optional[bool] = None
    errors_on_lint_failure: Optional[bool] = None

    # Unneeded read-only fields
    raw_dbt_version: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""
    deactivated: bool
    run_failure_count: int = False
    lifecycle_webhooks: bool = False
    lifecycle_webhooks_url: Optional[str] = None
    is_deferrable: bool = False
    generate_sources: bool = False
    cron_humanized: str = ""
    next_run: Optional[str] = ""
    next_run_humanized: Optional[str] = ""
    is_system: bool
    account: Any
    project: Any
    environment: Any
    most_recent_run: Any
    most_recent_completed_run: Any
