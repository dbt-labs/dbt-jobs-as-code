Both the `plan` and `sync` commands support a `--json` flag that outputs structured JSON to `stdout`, making it easy to consume the results in CI/CD pipelines and automation scripts.

!!! note
    Logs are printed to `stderr`, so to get clean JSON you can redirect `stderr` to `/dev/null` or redirect `stdout` to a file.

    ```bash
    dbt-jobs-as-code sync jobs.yml --json 2>/dev/null
    # or
    dbt-jobs-as-code sync jobs.yml --json > output.json 2>/dev/null
    ```

## Output schema

### `plan --json`

The `plan` command outputs a single JSON object with the planned changes:

```json
{
  "job_changes": [
    {
      "action": "CREATE",
      "type": "Job",
      "identifier": "daily_refresh",
      "project_id": 100,
      "environment_id": 200,
      "differences": {}
    }
  ],
  "env_var_overwrite_changes": [
    {
      "action": "UPDATE",
      "type": "Env Var Overwrite",
      "identifier": "daily_refresh:DBT_TARGET",
      "project_id": 100,
      "environment_id": 200,
      "differences": {"old_value": "dev", "new_value": "prod"}
    }
  ]
}
```

When there are no changes, both arrays are empty:

```json
{
  "job_changes": [],
  "env_var_overwrite_changes": []
}
```

### `sync --json`

The `sync` command outputs the same planned changes as `plan`, plus the results of the applied operations and a success flag:

```json
{
  "job_changes": [...],
  "env_var_overwrite_changes": [...],
  "applied": {
    "job_changes": [
      {
        "action": "CREATE",
        "type": "job",
        "identifier": "daily_refresh",
        "project_id": 100,
        "environment_id": 200,
        "job_id": 12345
      }
    ],
    "env_var_overwrite_changes": [
      {
        "action": "UPDATE",
        "type": "env var overwrite",
        "identifier": "daily_refresh:DBT_TARGET",
        "project_id": 100,
        "environment_id": 200,
        "env_var_id": 67890,
        "job_id": 12345
      }
    ]
  },
  "apply_success": true
}
```

The `applied` section contains the operations that were actually executed, including the `job_id` of the created/updated/deleted jobs. The `apply_success` field indicates whether all operations completed successfully.

## Using the JSON output in CI/CD

### Triggering jobs after sync

A common use case is to trigger newly created or updated jobs immediately after syncing. For example, using `jq` to extract the job IDs:

```bash
dbt-jobs-as-code sync jobs.yml --json 2>/dev/null | \
  jq -r '.applied.job_changes[].job_id' | sort -u | \
  xargs -I {} dbtc trigger-job --account-id 1 --job-id {}
```

### Posting plan results as a PR comment

The JSON output can be formatted and posted as a pull request comment for easy review:

```bash
dbt-jobs-as-code plan jobs.yml --json 2>/dev/null | \
  jq -r '.job_changes[] | "- \(.action) \(.type) \(.identifier)"'
```
