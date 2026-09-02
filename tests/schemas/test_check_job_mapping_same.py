from dbt_jobs_as_code.schemas import check_job_mapping_same
from dbt_jobs_as_code.schemas.job import JobDefinition


def test_check_job_mapping_same():
    mock_job1 = JobDefinition(
        id=1,
        name="Job 1",
        project_id=100,
        environment_id=200,
        account_id=300,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )
    mock_job2 = JobDefinition(
        id=2,
        name="Job 2",
        project_id=100,
        environment_id=400,
        account_id=300,
        deferring_environment_id=400,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )

    # Test that the jobs are different
    same, diff = check_job_mapping_same(mock_job1, mock_job2)
    assert not same
    assert diff is not None
    assert diff["status"] == "different"


def _make_job(**overrides):
    defaults = dict(
        name="Job",
        project_id=100,
        environment_id=200,
        account_id=300,
        settings={},
        run_generate_sources=False,
        execute_steps=[],
        generate_docs=False,
        schedule={"cron": "0 14 * * 0,1,2,3,4,5,6"},
        triggers={},
    )
    return JobDefinition(**{**defaults, **overrides})


class TestSelfDeferringEquivalence:
    """`deferring_job_definition_id` equal to the job's own id ("This Job" in the dbt
    Cloud UI) can be expressed two ways: the new `self_deferring` flag, or a literal
    id hardcoded in YAML (how people worked around this before the flag existed).
    Both must diff as identical to a cloud job that's self-deferring, and to each
    other -- and a real change must still be detected as different."""

    def test_legacy_hardcoded_id_matches_cloud_self_deferring(self):
        """Existing YAML that hardcodes the job's own id must not show as changed."""
        dest_job = _make_job(id=5, deferring_job_definition_id=5)
        source_job = _make_job(deferring_job_definition_id=5)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert same
        assert diff is None

    def test_new_flag_matches_cloud_self_deferring(self):
        """YAML using the new self_deferring flag must not show as changed either."""
        dest_job = _make_job(id=5, deferring_job_definition_id=5)
        source_job = _make_job(self_deferring=True)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert same
        assert diff is None

    def test_new_flag_matches_legacy_hardcoded_id_in_cloud(self):
        """Cloud job self-deferring via a plain int (set before this feature existed)
        must match YAML expressed with the new flag."""
        dest_job = _make_job(id=7, deferring_job_definition_id=7)
        source_job = _make_job(self_deferring=True)

        same, _diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert same

    def test_adding_self_deferring_is_detected_as_a_change(self):
        """Turning self_deferring on for a job that doesn't currently defer to
        itself must still surface as a real diff."""
        dest_job = _make_job(id=5, deferring_job_definition_id=None)
        source_job = _make_job(self_deferring=True)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert not same
        assert diff is not None

    def test_removing_self_deferring_is_detected_as_a_change(self):
        """Turning self_deferring off for a job that currently defers to itself in
        the cloud must still surface as a real diff."""
        dest_job = _make_job(id=5, deferring_job_definition_id=5)
        source_job = _make_job(self_deferring=False, deferring_job_definition_id=None)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert not same
        assert diff is not None

    def test_deferring_to_another_job_is_unaffected(self):
        """Normal cross-job deferring (not self-deferring) keeps working as before."""
        dest_job = _make_job(id=5, deferring_job_definition_id=999)
        source_job = _make_job(deferring_job_definition_id=999)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert same
        assert diff is None

    def test_switching_from_self_deferring_to_another_job_reports_the_real_target(self):
        """Cloud job currently self-defers; YAML wants it to defer to a *different*
        job (999). This must diff as different, and the reported differences must
        show the real target (999), not be erased by self-deferring normalization."""
        dest_job = _make_job(id=5, deferring_job_definition_id=5)
        source_job = _make_job(deferring_job_definition_id=999)

        same, diff = check_job_mapping_same(source_job=source_job, dest_job=dest_job)

        assert not same
        assert diff is not None
        differences = diff["differences"]
        assert "999" in str(differences)
