import pytest

from dbt_jobs_as_code.client import DBTCloud

service = DBTCloud(
    account_id=0,
    api_key="test_api_key",
)


# Define our test cases
@pytest.mark.parametrize(
    "project_ids, environment_id, offset, expected",
    [
        ([1], None, 10, {"offset": 10, "project_id": 1}),
        ([1, 2, 3], None, 20, {"offset": 20, "project_id__in": "[1,2,3]"}),
        ([1], 2, 30, {"offset": 30, "project_id": 1, "environment_id": 2}),
        ([1, 2], 3, 40, {"offset": 40, "project_id__in": "[1,2]", "environment_id": 3}),
        ([], None, 50, {"offset": 50}),
        ([], 4, 60, {"offset": 60, "environment_id": 4}),
        ([], None, 0, {"offset": 0}),
    ],
)
def test_build_parameters(project_ids, environment_id, offset, expected):
    result = service._build_parameters(project_ids, environment_id, offset)
    assert result == expected
