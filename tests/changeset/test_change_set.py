from unittest.mock import Mock

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
