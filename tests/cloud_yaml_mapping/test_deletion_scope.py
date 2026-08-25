from unittest.mock import patch

from dbt_jobs_as_code.cloud_yaml_mapping.change_set import (
    RunScope,
    _account_id_for_run,
    _deletion_scope_is_valid,
)


class MockJob:
    def __init__(self, account_id):
        self.account_id = account_id


def _scope(
    defined_jobs=None,
    all_defined_jobs=None,
    project_ids=None,
    environment_ids=None,
    account_id=None,
):
    return RunScope(
        defined_jobs=defined_jobs or {},
        all_defined_jobs=all_defined_jobs or {},
        project_ids=project_ids or [],
        environment_ids=environment_ids or [],
        account_id=account_id,
        config="jobs.yml",
        config_files=["jobs.yml"],
        yml_vars_files=None,
    )


# -- _deletion_scope_is_valid --


def test_deletion_scope_valid_when_defined_jobs_non_empty():
    scope = _scope(defined_jobs={"a": MockJob(account_id=1)})
    assert _deletion_scope_is_valid(scope) is True


def test_deletion_scope_valid_when_all_defined_jobs_scoped_to_project_and_environment():
    scope = _scope(
        all_defined_jobs={"a": MockJob(account_id=1)},
        project_ids=[123],
        environment_ids=[456],
    )
    assert _deletion_scope_is_valid(scope) is True


def test_deletion_scope_invalid_when_all_defined_jobs_not_scoped():
    scope = _scope(all_defined_jobs={"a": MockJob(account_id=1)})
    assert _deletion_scope_is_valid(scope) is False


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.any_file_declares_jobs_key")
def test_deletion_scope_invalid_when_no_file_declares_jobs_key(mock_declares_jobs_key):
    mock_declares_jobs_key.return_value = False
    scope = _scope(project_ids=[123], environment_ids=[456], account_id=789)
    assert _deletion_scope_is_valid(scope) is False


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.any_file_declares_jobs_key")
def test_deletion_scope_valid_when_emptied_and_scoped_with_account_id(mock_declares_jobs_key):
    mock_declares_jobs_key.return_value = True
    scope = _scope(project_ids=[123], environment_ids=[456], account_id=789)
    assert _deletion_scope_is_valid(scope) is True


@patch("dbt_jobs_as_code.cloud_yaml_mapping.change_set.any_file_declares_jobs_key")
def test_deletion_scope_invalid_when_emptied_without_account_id(mock_declares_jobs_key):
    mock_declares_jobs_key.return_value = True
    scope = _scope(project_ids=[123], environment_ids=[456])
    assert _deletion_scope_is_valid(scope) is False


# -- _account_id_for_run --


def test_account_id_for_run_from_defined_jobs():
    scope = _scope(defined_jobs={"a": MockJob(account_id=111)})
    assert _account_id_for_run(scope) == 111


def test_account_id_for_run_from_all_defined_jobs():
    scope = _scope(all_defined_jobs={"a": MockJob(account_id=222)})
    assert _account_id_for_run(scope) == 222


def test_account_id_for_run_from_account_id_override():
    scope = _scope(account_id=333)
    assert _account_id_for_run(scope) == 333
