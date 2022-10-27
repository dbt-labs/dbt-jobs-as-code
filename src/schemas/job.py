import pydantic

from typing import Any, Dict, List, Optional

class JobDefinition(pydantic.BaseModel):
    identifier: str
    account_id: int
    dbt_version: str = "1.3.1"
    deferring_job_definition_id: int
    environment_id: int
    execute_steps: List[str] = ["dbt run",]
    execution: Dict # current supports timeout_seconds
    generate_docs: bool
    generate_sources: bool
    name: str = "New Job"
    project_id: int
    run_generate_sources: bool
    schedule: Optional[Dict] # TODO figure out Schedule type
    settings: Dict[str: Any] # TODO figure out Union type
    triggers: Dict[str, bool]
    """
      e.g.
      custom_branch_only: true
      git_provider_webhook: false
      github_webhook: false
      schedule: true
    """
    custom_environment_variables: List # TODO define CEV type

