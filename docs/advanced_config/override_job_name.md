# Override UI Job Name

By default, `dbt-jobs-as-code` appends a `[[identifier]]` suffix to each job's name in dbt Cloud (e.g. `Daily Job [[daily_job_prod]]`). This suffix is how the tool tracks which YAML definition maps to which Cloud job.

If you want the job name in the dbt Cloud UI to be **exactly** what you specify (without the `[[identifier]]` suffix), add the `ui_job_name_override` field.

## Configuration

```yaml
jobs:
  carter_job:
    account_id: 77338
    project_id: 136022
    environment_id: 126992
    name: Carter Job
    ui_job_name_override: Carter Job
    # ... rest of job config
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ui_job_name_override` | string or null | `null` | When set, this exact name is used in dbt Cloud without the `[[identifier]]` suffix. |

## How it works

- **Without override (default):** The job is created in dbt Cloud as `Carter Job [[carter_job]]`. The `[[carter_job]]` suffix is used to track the job across syncs.
- **With override:** The job is created in dbt Cloud as `Carter Job` (no suffix). The tool matches the job on subsequent syncs by looking for a cloud job with the same name, project ID, and environment ID.

## Example

```yaml
jobs:
  # Standard job -- will appear as "Nightly Job [[nightly_job]]" in dbt Cloud
  nightly_job:
    name: Nightly Job
    # ...

  # Override job -- will appear as exactly "Carter Job" in dbt Cloud
  carter_job:
    name: Carter Job
    ui_job_name_override: Carter Job
    # ...
```

## Important notes

- When `ui_job_name_override` is absent or null (the default), the tool behaves exactly as before. Existing YAML files require no changes.
- Since override jobs are matched by name rather than `[[identifier]]`, the `ui_job_name_override` value should be unique within a given project and environment to avoid ambiguous matches.
