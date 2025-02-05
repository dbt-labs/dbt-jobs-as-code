import pytest
from pydantic import ValidationError

from dbt_jobs_as_code.schemas.custom_environment_variable import (
    CustomEnvironmentVariable,
)


def test_custom_env_var_validation():
    """Test that environment variable validation works correctly"""

    # Valid cases
    valid_var = CustomEnvironmentVariable(name="DBT_TEST_VAR", value="test_value")
    assert valid_var.name == "DBT_TEST_VAR"
    assert valid_var.value == "test_value"
    assert valid_var.type == "job"

    # Test invalid prefix
    with pytest.raises(ValidationError) as exc:
        CustomEnvironmentVariable(name="TEST_VAR", value="test")
    assert "Key must have `DBT_` prefix" in str(exc.value)

    # Test lowercase name
    with pytest.raises(ValidationError) as exc:
        CustomEnvironmentVariable(name="DBT_test_var", value="test")
    assert "Key name must be SCREAMING_SNAKE_CASE" in str(exc.value)
