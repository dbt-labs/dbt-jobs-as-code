# Advanced configuration

More advanced features of `dbt-jobs-as-code` can be combined to match complex requirements in regards to dbt Cloud jobs management.

- [YAML Templating](templating.md) - for using the same YAML file to update different dbt Cloud projects and/or environments
- [glob config files](glob_config_files.md) - for using glob patterns to match config files at once
- [YAML anchors](yaml_anchors.md) - to reuse the same parameters in different jobs
- [Advanced jobs importing](jobs_importing.md) - for importing jobs from dbt Cloud to a YAML file
- [JSON output](json_output.md) - for consuming `plan` and `sync` results in automation scripts
- [Override UI Job Name](override_job_name.md) - for controlling the exact job name shown in the dbt Cloud UI without the `[[identifier]]` suffix
