import pytest

from src.client import DBTCloud


def test_check_for_creds_no_api_key():
    dbt_client = DBTCloud(account_id=123, api_key=None)
    with pytest.raises(Exception, match="An API key is required to get dbt Cloud jobs."):
        dbt_client._check_for_creds()

def test_check_for_creds_no_account_id():
    dbt_client = DBTCloud(account_id=None, api_key="test_api_key")
    with pytest.raises(Exception, match="An account_id is required to get dbt Cloud jobs."):
        dbt_client._check_for_creds()

def test_check_for_creds_with_creds():
    dbt_client = DBTCloud(account_id=123, api_key="test_api_key")
    try:
        dbt_client._check_for_creds()
    except Exception:
        pytest.fail("Unexpected exception raised.")