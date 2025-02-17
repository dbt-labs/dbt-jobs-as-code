import pytest

from dbt_jobs_as_code.loader.load import (
    LoadingJobsYAMLError,
    _load_vars_files,
    _load_yaml_no_template,
    _load_yaml_with_template,
    load_job_configuration,
    resolve_file_paths,
)

# all the different ways of defining a set of jobs still create the same Pydantic jobs


class TestLoaderLoadJobConfiguration:
    def test_load_yml_no_anchor(self, expected_config_dict):
        """Test that loading configuration without YML anchors works as expected."""
        loaded_config = load_job_configuration(["tests/loader/jobs.yml"], None)
        assert loaded_config.model_dump() == expected_config_dict

    def test_load_yml_anchors(self, expected_config_dict):
        """Test that loading configuration with YML anchors works as expected."""
        loaded_config = load_job_configuration(["tests/loader/jobs_with_anchors.yml"], None)
        assert loaded_config.model_dump() == expected_config_dict

    def test_load_yml_templated(self, expected_config_dict):
        """Test that load_job_configuration works with templated YAML and variables."""
        loaded_config = load_job_configuration(
            ["tests/loader/jobs_templated.yml"], ["tests/loader/jobs_templated_vars.yml"]
        )
        assert loaded_config.model_dump() == expected_config_dict

    def test_error_load_yml_templated_missing_vars_parameter(self):
        """Test that load_job_configuration works with templated YAML and variables."""

        with pytest.raises(LoadingJobsYAMLError) as exc_info:
            load_job_configuration(
                ["tests/loader/jobs_templated.yml"],
                ["tests/loader/jobs_templated_vars_missing.yml"],
            )

        # we check that the error messages contains the missing variables
        assert "'environment_id'" in str(exc_info.value)

    def test_error_load_yml_templated_missing_specific_var(self):
        """Test that load_job_configuration works with templated YAML and variables."""

        with pytest.raises(LoadingJobsYAMLError) as exc_info:
            load_job_configuration(["tests/loader/jobs_templated.yml"], None)

        # we check that the error messages contains the missing variables
        assert "'environment_id'" in str(exc_info.value)
        assert "'project_id'" in str(exc_info.value)

    def test_load_job_configuration_empty_jobs(self, tmp_path):
        """Test loading a config file with no jobs"""
        config_file = tmp_path / "empty.yml"
        config_file.write_text("jobs: {}")

        result = load_job_configuration([str(config_file)], None)
        assert result.jobs == {}


class TestLoaderLoadYamlWithTemplate:
    def test_load_yaml_with_template_single_file(self, config_files, vars_files):
        """Test loading a single templated YAML file with a single vars file"""
        result = _load_yaml_with_template(config_files, vars_files)

        assert result == {"jobs": {"job1": {"project_id": 123, "environment_id": 456}}}

    def test_load_yaml_with_template_multiple_files(
        self, multiple_config_files, multiple_vars_files
    ):
        """Test loading multiple config files with multiple vars files"""
        result = _load_yaml_with_template(multiple_config_files, multiple_vars_files)

        assert result == {"jobs": {"job1": {"project_id": 123}, "job2": {"environment_id": 456}}}

    def test_load_yaml_with_template_duplicate_vars(self, tmp_path, config_files):
        """Test error when vars files contain duplicate variables"""
        vars1 = tmp_path / "vars1.yml"
        vars2 = tmp_path / "vars2.yml"

        vars1.write_text("project_id: 123")
        vars2.write_text("project_id: 456")

        with pytest.raises(
            LoadingJobsYAMLError, match="Variable 'project_id' is defined multiple times"
        ):
            _load_yaml_with_template(config_files, [str(vars1), str(vars2)])

    def test_load_yaml_with_template_undefined_var(self, tmp_path):
        """Test error when template contains undefined variables"""
        config = tmp_path / "config.yml"
        vars_file = tmp_path / "vars.yml"

        config.write_text("""
jobs:
    job1:
        project_id: {{ undefined_var }}
    """)
        vars_file.write_text("project_id: 123")

        with pytest.raises(LoadingJobsYAMLError, match="Some variables didn't have a value"):
            _load_yaml_with_template([str(config)], [str(vars_file)])

    def test_load_yaml_with_template_empty_files(self, tmp_path):
        """Test handling of empty config and vars files"""
        config = tmp_path / "empty.yml"
        vars_file = tmp_path / "empty_vars.yml"

        config.write_text("")
        vars_file.write_text("")

        result = _load_yaml_with_template([str(config)], [str(vars_file)])
        assert result == {}

    def test_load_yaml_with_template_merge_jobs(self, tmp_path):
        """Test merging of jobs from multiple config files"""
        config1 = tmp_path / "config1.yml"
        config2 = tmp_path / "config2.yml"
        vars_file = tmp_path / "vars.yml"

        config1.write_text("""
jobs:
    job1:
        value: {{ val1 }}
    """)

        config2.write_text("""
jobs:
    job2:
        value: {{ val2 }}
    """)

        vars_file.write_text("""
val1: 123
val2: 456
    """)

        result = _load_yaml_with_template([str(config1), str(config2)], [str(vars_file)])

        assert result == {"jobs": {"job1": {"value": 123}, "job2": {"value": 456}}}

    def test_load_yaml_with_template_nested_field(self, tmp_path):
        """Test error when template contains undefined variables"""
        config = tmp_path / "config.yml"
        vars_file = tmp_path / "vars.yml"

        config.write_text("""
jobs:
    job1:
        schedule: {{ schedule }}
    """)
        vars_file.write_text("""
schedule:
  cron: 0 1,5 * * 0,1,2,3,4,5""")

        result = _load_yaml_with_template([str(config)], [str(vars_file)])
        assert result == {"jobs": {"job1": {"schedule": {"cron": "0 1,5 * * 0,1,2,3,4,5"}}}}


class TestLoaderResolveFilePaths:
    def test_resolve_file_paths_no_config(self):
        """Test when no config pattern is provided"""
        config_files, vars_files = resolve_file_paths(None)
        assert config_files == []
        assert vars_files == []

    def test_resolve_file_paths_single_file(self, tmp_path):
        """Test resolving a single config file"""
        config_file = tmp_path / "config.yml"
        config_file.write_text("content")

        config_files, vars_files = resolve_file_paths(str(config_file))
        assert config_files == [str(config_file)]
        assert vars_files == []

    def test_resolve_file_paths_with_glob(self, tmp_path):
        """Test resolving multiple config files using glob pattern"""
        # Create test files
        (tmp_path / "config1.yml").write_text("content1")
        (tmp_path / "config2.yml").write_text("content2")
        (tmp_path / "other.txt").write_text("other")

        config_files, vars_files = resolve_file_paths(str(tmp_path / "*.yml"))
        assert len(config_files) == 2
        assert all(f.endswith(".yml") for f in config_files)
        assert vars_files == []

    def test_resolve_file_paths_with_vars(self, tmp_path):
        """Test resolving both config and vars files"""
        config_file = tmp_path / "config.yml"
        vars_file = tmp_path / "vars.yml"
        config_file.write_text("content")
        vars_file.write_text("vars")

        config_files, vars_files = resolve_file_paths(str(config_file), str(vars_file))
        assert config_files == [str(config_file)]
        assert vars_files == [str(vars_file)]

    def test_resolve_file_paths_no_matches(self, tmp_path):
        """Test error when no files match the pattern"""
        with pytest.raises(LoadingJobsYAMLError, match="No files found matching pattern"):
            resolve_file_paths(str(tmp_path / "nonexistent*.yml"))

    def test_resolve_file_paths_no_vars_matches(self, tmp_path):
        """Test error when no vars files match the pattern"""
        config_file = tmp_path / "config.yml"
        config_file.write_text("content")

        with pytest.raises(LoadingJobsYAMLError, match="No files found matching pattern"):
            resolve_file_paths(str(config_file), str(tmp_path / "nonexistent*.yml"))

    def test_resolve_file_paths_multiple_vars(self, tmp_path):
        """Test resolving multiple vars files"""
        # Create test files
        config_file = tmp_path / "config.yml"
        config_file.write_text("content")
        (tmp_path / "vars1.yml").write_text("vars1")
        (tmp_path / "vars2.yml").write_text("vars2")

        config_files, vars_files = resolve_file_paths(
            str(config_file), str(tmp_path / "vars*.yml")
        )
        assert config_files == [str(config_file)]
        assert len(vars_files) == 2
        assert all(f.endswith(".yml") for f in vars_files)


class TestLoaderLoadYamlNoTemplate:
    def test_load_yaml_no_template_multiple_files(self, tmp_path):
        """Test merging multiple non-templated config files"""
        config1 = tmp_path / "config1.yml"
        config2 = tmp_path / "config2.yml"

        config1.write_text("""
jobs:
    job1:
        name: Job 1
        """)

        config2.write_text("""
jobs:
    job2:
        name: Job 2
        """)

        result = _load_yaml_no_template([str(config1), str(config2)])
        assert "job1" in result["jobs"]
        assert "job2" in result["jobs"]


class TestLoaderLoadVarsFiles:
    def test_load_vars_files_single_file(self, tmp_path):
        """Test loading a single vars file"""
        vars_file = tmp_path / "vars.yml"
        vars_file.write_text("""
project_id: 123
environment_id: 456
        """)

        result = _load_vars_files([str(vars_file)])

        assert result == {"project_id": 123, "environment_id": 456}

    def test_load_vars_files_multiple_files(self, tmp_path):
        """Test loading multiple vars files with different variables"""
        vars1 = tmp_path / "vars1.yml"
        vars2 = tmp_path / "vars2.yml"

        vars1.write_text("project_id: 123")
        vars2.write_text("environment_id: 456")

        result = _load_vars_files([str(vars1), str(vars2)])

        assert result == {"project_id": 123, "environment_id": 456}

    def test_load_vars_files_empty_file(self, tmp_path):
        """Test loading an empty vars file"""
        vars_file = tmp_path / "empty.yml"
        vars_file.write_text("")

        result = _load_vars_files([str(vars_file)])

        assert result == {}

    def test_load_vars_files_duplicate_vars(self, tmp_path):
        """Test error when vars files contain duplicate variables"""
        vars1 = tmp_path / "vars1.yml"
        vars2 = tmp_path / "vars2.yml"

        vars1.write_text("project_id: 123")
        vars2.write_text("project_id: 456")

        with pytest.raises(
            LoadingJobsYAMLError, match="Variable 'project_id' is defined multiple times"
        ):
            _load_vars_files([str(vars1), str(vars2)])

    def test_load_vars_files_no_files(self):
        """Test loading with empty list of files"""
        result = _load_vars_files([])

        assert result == {}

    def test_load_vars_files_nested_vars(self, tmp_path):
        """Test loading a single vars file"""
        vars_file = tmp_path / "vars.yml"
        vars_file.write_text("""
schedule:
  cron: 0 1,5 * * 0,1,2,3,4,5
        """)

        result = _load_vars_files([str(vars_file)])

        assert result == {"schedule": {"cron": "0 1,5 * * 0,1,2,3,4,5"}}

    def test_load_vars_files_none_values(self, tmp_path):
        """Test that None values in vars files are correctly replaced with 'null' strings"""
        vars_file = tmp_path / "vars.yml"
        vars_file.write_text("""
string_var: value
none_var: null
nested_dict:
    none_field: null
list_with_none:
    - item1
    - null
    - item3
nested_list:
    - name: item1
      value: null
    - name: item2
      value: not_null
        """)

        result = _load_vars_files([str(vars_file)])

        assert result["string_var"] == "value"
        assert result["none_var"] == "null"
        assert result["nested_dict"]["none_field"] == "null"
        assert result["list_with_none"] == ["item1", "null", "item3"]
        assert result["nested_list"] == [
            {"name": "item1", "value": "null"},
            {"name": "item2", "value": "not_null"},
        ]
