from unittest.mock import Mock

from dbt_jobs_as_code.client import DBTCloudException
from dbt_jobs_as_code.cloud_yaml_mapping.change_set import Change, ChangeSet


def test_change_set_to_json_empty():
    """Test that an empty change set produces correct JSON structure"""
    change_set = ChangeSet()
    json_output = change_set.to_json()

    assert json_output == {
        "job_changes": [],
        "env_var_overwrite_changes": [],
    }


def test_change_set_to_json_job_changes():
    """Test that job changes are correctly represented in JSON"""
    change_set = ChangeSet()

    # Add a CREATE job change
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
        )
    )

    # Add an UPDATE job change with differences
    change_set.append(
        Change(
            identifier="job2",
            type="job",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
            differences={
                "values_changed": {
                    "root['name']": {"new_value": "new_name", "old_value": "old_name"}
                }
            },
        )
    )

    # Add a DELETE job change
    change_set.append(
        Change(
            identifier="job3",
            type="job",
            action="delete",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
        )
    )

    json_output = change_set.to_json()

    assert len(json_output["job_changes"]) == 3
    assert len(json_output["env_var_overwrite_changes"]) == 0

    # Verify CREATE job
    create_job = next(c for c in json_output["job_changes"] if c["action"] == "CREATE")
    assert create_job["identifier"] == "job1"
    assert create_job["project_id"] == 123
    assert create_job["environment_id"] == 456

    # Verify UPDATE job with differences
    update_job = next(c for c in json_output["job_changes"] if c["action"] == "UPDATE")
    assert update_job["identifier"] == "job2"
    assert "differences" in update_job
    assert update_job["differences"]["values_changed"]["root['name']"]["new_value"] == "new_name"

    # Verify DELETE job
    delete_job = next(c for c in json_output["job_changes"] if c["action"] == "DELETE")
    assert delete_job["identifier"] == "job3"


def test_change_set_to_json_env_var_changes():
    """Test that environment variable changes are correctly represented in JSON"""
    change_set = ChangeSet()

    # Add a CREATE env var change
    change_set.append(
        Change(
            identifier="job1:DBT_VAR1",
            type="env var overwrite",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
        )
    )

    # Add an UPDATE env var change with differences
    change_set.append(
        Change(
            identifier="job1:DBT_VAR2",
            type="env var overwrite",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
            differences={"old_value": "old_val", "new_value": "new_val"},
        )
    )

    # Add a DELETE env var change
    change_set.append(
        Change(
            identifier="job1:DBT_VAR3",
            type="env var overwrite",
            action="delete",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
        )
    )

    json_output = change_set.to_json()

    assert len(json_output["job_changes"]) == 0
    assert len(json_output["env_var_overwrite_changes"]) == 3

    # Verify CREATE env var
    create_var = next(
        c for c in json_output["env_var_overwrite_changes"] if c["action"] == "CREATE"
    )
    assert create_var["identifier"] == "job1:DBT_VAR1"
    assert create_var["project_id"] == 123
    assert create_var["environment_id"] == 456

    # Verify UPDATE env var with differences
    update_var = next(
        c for c in json_output["env_var_overwrite_changes"] if c["action"] == "UPDATE"
    )
    assert update_var["identifier"] == "job1:DBT_VAR2"
    assert "differences" in update_var
    assert update_var["differences"]["old_value"] == "old_val"
    assert update_var["differences"]["new_value"] == "new_val"

    # Verify DELETE env var
    delete_var = next(
        c for c in json_output["env_var_overwrite_changes"] if c["action"] == "DELETE"
    )
    assert delete_var["identifier"] == "job1:DBT_VAR3"


def test_change_set_to_json_mixed_changes():
    """Test that a mix of job and env var changes are correctly represented in JSON"""
    change_set = ChangeSet()

    # Add a job change
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
            differences={
                "values_changed": {
                    "root['name']": {"new_value": "new_name", "old_value": "old_name"}
                }
            },
        )
    )

    # Add an env var change
    change_set.append(
        Change(
            identifier="job1:DBT_VAR1",
            type="env var overwrite",
            action="update",
            proj_id=123,
            env_id=456,
            sync_function=Mock(),
            parameters={},
            differences={"old_value": "old_val", "new_value": "new_val"},
        )
    )

    json_output = change_set.to_json()

    assert len(json_output["job_changes"]) == 1
    assert len(json_output["env_var_overwrite_changes"]) == 1

    # Verify job change
    job_change = json_output["job_changes"][0]
    assert job_change["identifier"] == "job1"
    assert "differences" in job_change

    # Verify env var change
    env_var_change = json_output["env_var_overwrite_changes"][0]
    assert env_var_change["identifier"] == "job1:DBT_VAR1"
    assert "differences" in env_var_change


def test_change_set_apply_fail_fast():
    """Test that ChangeSet.apply() stops on first failure when fail_fast=True"""
    change_set = ChangeSet()

    # Mock functions to simulate different behaviors
    successful_mock = Mock()
    failing_mock = Mock(side_effect=DBTCloudException("Test error"))
    should_not_be_called_mock = Mock()

    # Add three changes
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=successful_mock,
            parameters={},
        )
    )

    change_set.append(
        Change(
            identifier="job2",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=failing_mock,
            parameters={},
        )
    )

    change_set.append(
        Change(
            identifier="job3",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=should_not_be_called_mock,
            parameters={},
        )
    )

    # Apply with fail_fast=True
    change_set.apply(fail_fast=True)

    # Verify first function was called, second failed, third was not called
    successful_mock.assert_called_once()
    failing_mock.assert_called_once()
    should_not_be_called_mock.assert_not_called()

    # Verify apply_success is False
    assert change_set.apply_success is False


def test_change_set_apply_no_fail_fast():
    """Test that ChangeSet.apply() continues on failure when fail_fast=False"""
    change_set = ChangeSet()

    # Mock functions to simulate different behaviors
    successful_mock = Mock()
    failing_mock = Mock(side_effect=DBTCloudException("Test error"))
    should_be_called_mock = Mock()

    # Add three changes
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=successful_mock,
            parameters={},
        )
    )

    change_set.append(
        Change(
            identifier="job2",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=failing_mock,
            parameters={},
        )
    )

    change_set.append(
        Change(
            identifier="job3",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=should_be_called_mock,
            parameters={},
        )
    )

    # Apply with fail_fast=False (default)
    change_set.apply(fail_fast=False)

    # Verify all functions were called
    successful_mock.assert_called_once()
    failing_mock.assert_called_once()
    should_be_called_mock.assert_called_once()

    # Verify apply_success is False due to the failure
    assert change_set.apply_success is False


def test_change_set_apply_all_success():
    """Test that ChangeSet.apply() works correctly when all changes succeed"""
    change_set = ChangeSet()

    # Mock functions that succeed
    successful_mock1 = Mock()
    successful_mock2 = Mock()

    # Add two changes
    change_set.append(
        Change(
            identifier="job1",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=successful_mock1,
            parameters={},
        )
    )

    change_set.append(
        Change(
            identifier="job2",
            type="job",
            action="create",
            proj_id=123,
            env_id=456,
            sync_function=successful_mock2,
            parameters={},
        )
    )

    # Apply with fail_fast=True
    change_set.apply(fail_fast=True)

    # Verify all functions were called
    successful_mock1.assert_called_once()
    successful_mock2.assert_called_once()

    # Verify apply_success is True
    assert change_set.apply_success is True


def test_change_apply_captures_job_id_on_create():
    """Test that Change.apply() captures job_id when creating a job"""
    # Mock JobDefinition return value
    mock_job = Mock()
    mock_job.id = 12345

    mock_sync_function = Mock(return_value=mock_job)

    change = Change(
        identifier="test_job",
        type="job",
        action="create",
        proj_id=123,
        env_id=456,
        sync_function=mock_sync_function,
        parameters={},
    )

    # Apply the change
    change.apply()

    # Verify the job ID was captured
    assert change.result_job_id == 12345
    mock_sync_function.assert_called_once()


def test_change_set_to_completed_operations_json_empty():
    """Test that an empty change set produces correct completed operations JSON"""
    change_set = ChangeSet()
    json_output = change_set.to_completed_operations_json()

    assert json_output == {
        "completed_operations": [],
    }


def test_change_set_to_completed_operations_json_job_operations():
    """Test that completed job operations are correctly represented in JSON"""
    change_set = ChangeSet()

    # Mock return values for job operations
    create_job_mock = Mock()
    create_job_mock.id = 111
    create_sync = Mock(return_value=create_job_mock)

    update_job_mock = Mock()
    update_job_mock.id = 222
    update_sync = Mock(return_value=update_job_mock)

    delete_job_mock = Mock()
    delete_job_mock.id = 333
    delete_sync = Mock(return_value=None)

    # Add CREATE job change
    create_change = Change(
        identifier="job1",
        type="job",
        action="create",
        proj_id=123,
        env_id=456,
        sync_function=create_sync,
        parameters={},
    )
    change_set.append(create_change)

    # Add UPDATE job change
    update_change = Change(
        identifier="job2",
        type="job",
        action="update",
        proj_id=789,
        env_id=101,
        sync_function=update_sync,
        parameters={},
    )
    change_set.append(update_change)

    # Add DELETE job change
    delete_change = Change(
        identifier="job3",
        type="job",
        action="delete",
        proj_id=111,
        env_id=222,
        sync_function=delete_sync,
        parameters={"job": delete_job_mock},
    )
    change_set.append(delete_change)

    # Apply the changes to capture job IDs
    change_set.apply()

    # Get completed operations JSON
    json_output = change_set.to_completed_operations_json()

    assert len(json_output["completed_operations"]) == 3

    # Verify CREATE job
    create_op = next(op for op in json_output["completed_operations"] if op["action"] == "CREATE")
    assert create_op["identifier"] == "job1"
    assert create_op["job_id"] == 111
    assert create_op["project_id"] == 123
    assert create_op["environment_id"] == 456
    assert create_op["type"] == "Job"

    # Verify UPDATE job
    update_op = next(op for op in json_output["completed_operations"] if op["action"] == "UPDATE")
    assert update_op["identifier"] == "job2"
    assert update_op["job_id"] == 222
    assert update_op["project_id"] == 789
    assert update_op["environment_id"] == 101

    # Verify DELETE job
    delete_op = next(op for op in json_output["completed_operations"] if op["action"] == "DELETE")
    assert delete_op["identifier"] == "job3"
    assert delete_op["job_id"] == 333
    assert delete_op["project_id"] == 111
    assert delete_op["environment_id"] == 222


