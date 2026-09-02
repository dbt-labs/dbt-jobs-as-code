
To see the details of all changes, head to the GitHub repo

### 1.21

- Add `self_deferring` to job definitions, to configure a job to defer to its own last successful run ("This Job" in the dbt Cloud UI) without hardcoding the job's own dbt Cloud ID.

### 1.20

- Internal: modernized the lint/type-checking tooling (expanded the `ruff` rule set, added Astral's `ty` type checker, dropped an unneeded `beartype.typing` compatibility shim, enforced these in CI). No user-facing changes.

### 1.19

- `plan`/`sync` can now reconcile job deletions when jobs are removed from the YAML (individually, or by emptying the `jobs` key entirely), as long as the command is scoped with `--project-id` and `--environment-id`. See [glob config files](advanced_config/glob_config_files.md) for the different scenarios.
- Add an `--account-id` flag to `plan` and `sync`, needed to reconcile deletions when the config declares no jobs at all.
- Fix `validate --online` crashing with an `IndexError` when the config declares no jobs.

### 1.18

- Add `--use-desc-for-id` flag (env var: `DBT_JOBS_AS_CODE_USE_DESC_FOR_ID`) to store the `[[<identifier>]]` tag in the job description instead of the job name. This is useful when keeping job names clean in the dbt Cloud UI is a requirement. Supported by all commands: `plan`, `sync`, `validate`, `import-jobs`, `link`, `unlink`, `deactivate-jobs`.

### 1.17

- Validate that job descriptions don't exceed the 255 character limit before sending to the API.
- Drop Python 3.9 support (EOL) and update dependencies.

### 1.16

- Add support for `cost_optimization_features` in job definitions. Valid values are `state_aware_orchestration` and `efficient_testing`. This allows for dbt users on the Fusion engine to configure cost optimization natively in their YAML job definitions.

### 1.14

- Add applied job IDs to `sync --json` output. The JSON now includes an `applied` section with `job_id` for each operation and an `apply_success` flag. See [JSON output](advanced_config/json_output.md) for details.

### 1.6

- Add `--filter` to `import-jobs` to allow importing jobs to specific environments. In the case where people maintain jobs in the dbt Cloud UI and want to promote them, they can mention what environments they want to import the jobs to using the identifier of the job: `[[envs_filter:identifier]]`.

### 1.5

- Add `--json` to `plan` and `sync` to output the `stdout` changes in JSON format. This can be useful for automating some processes and consuming the changes from scripts. We are still printing logs to `stderr` though, so to remove those logs you can redirect `stderr` to `/dev/null` or redirect `stdout` to a file and then read from the file.

### 1.4

- Add `--templated-fields` to `import-jobs` to add Jinja variables to the generated YAML file. This can be useful to allow users to maintain jobs in the dbt Cloud UI and set a process to automatically promote those to other environments.

### 1.3

- Add this docs site
- Add `--managed-only` flag to the `import` command to only import managed jobs
- Add `--environment-id` and `--project-id` flags to `link`, `unlink` and `deactivate-jobs` commands

### 1.2

- Automatically set the identifier when using `import-jobs` on managed jobs. This automatically links the jobs to the generated YAML file.

### 1.1

- Add the ability to mention "glob" files for the YAML config and var files.
    - i.e. `dbt-jobs-as-code plan ".dbt/jobs/*"` can be used to take into consideration all files in the `.dbt/jobs` directory.

### 1.0

- Initial release of `1.0`