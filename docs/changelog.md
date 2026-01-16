
To see the details of all changes, head to the GitHub repo

### 1.7

**New Features:**

- Add support for State-Aware Orchestration (SAO) fields:
  - `cost_optimization_features` - New preferred method to enable SAO. Set to `["state_aware_orchestration"]` to enable.
  - `force_node_selection` - Legacy method (deprecated). Set to `false` or `null` to enable SAO.
- Automatic handling of CI/Merge jobs: `force_node_selection` is automatically omitted from API payloads for CI and Merge jobs, as the dbt Cloud API rejects this field for these job types.
- SAO fields are only exported when they have non-default values, keeping exported YAML clean.
- Added comprehensive [SAO documentation](advanced_config/sao.md).

**Adjusting Existing Configurations:**

No changes are required for existing configurations - the new fields have sensible defaults:
- `force_node_selection: null` (SAO behavior determined by dbt Cloud)
- `cost_optimization_features: []` (no optimizations enabled)

To enable SAO on existing scheduled jobs (requires `dbt_version: "latest-fusion"`):

```yaml
# Option 1: Recommended - use cost_optimization_features
jobs:
  my_job:
    dbt_version: "latest-fusion"
    cost_optimization_features:
      - state_aware_orchestration
    # ... rest of config

# Option 2: Legacy - use force_node_selection (deprecated)
jobs:
  my_job:
    dbt_version: "latest-fusion"
    force_node_selection: false
    # ... rest of config
```

**Important Notes:**
- SAO is only available for scheduled jobs (not CI or Merge jobs)
- SAO requires `dbt_version: "latest-fusion"`
- For CI/Merge jobs, do not set `force_node_selection` - it will be automatically omitted

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