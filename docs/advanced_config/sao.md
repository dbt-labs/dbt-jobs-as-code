# State-Aware Orchestration (SAO)

State-Aware Orchestration (SAO) is a dbt Cloud feature that optimizes job execution by intelligently selecting which nodes to run based on their state. This can significantly reduce job execution time and resource usage.

## Prerequisites

SAO requires:

- **dbt version**: `latest-fusion` (SAO is only available on Fusion runtime)
- **Job type**: Scheduled jobs only (CI/Merge jobs do not support SAO)

## Configuration Options

There are two ways to enable SAO in your job configuration:

### Recommended: `cost_optimization_features`

The preferred method is to use the `cost_optimization_features` field:

```yaml
jobs:
  my_optimized_job:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    dbt_version: "latest-fusion"  # Required for SAO
    name: "Production Build with SAO"
    execute_steps:
      - dbt build
    # ... other required fields ...
    cost_optimization_features:
      - state_aware_orchestration
```

Valid values for `cost_optimization_features`:
- `state_aware_orchestration` - Enables SAO

### Deprecated: `force_node_selection`

The legacy method uses the `force_node_selection` field:

```yaml
jobs:
  my_optimized_job:
    # ... other fields ...
    force_node_selection: false  # false or null enables SAO
```

| Value | Effect |
|-------|--------|
| `false` or `null` | SAO enabled (on Fusion runtime) |
| `true` | SAO disabled |

> **Note**: `force_node_selection` is deprecated. Use `cost_optimization_features` for new configurations.

## CI/Merge Jobs

SAO is **not available** for CI or Merge jobs. The dbt Cloud API will reject job configurations that attempt to set `force_node_selection` for these job types.

dbt-jobs-as-code automatically handles this by omitting `force_node_selection` from the API payload when:

- `triggers.github_webhook` is `true`
- `triggers.git_provider_webhook` is `true`
- `triggers.on_merge` is `true`
- `job_type` is `"ci"` or `"merge"`

## Adjusting Existing Configurations

### Upgrading from dbt-jobs-as-code < 1.7

**No action required!** Existing job configurations continue to work without modification. The new SAO fields have sensible defaults:

- `force_node_selection`: `null` (SAO behavior determined by dbt Cloud defaults)
- `cost_optimization_features`: `[]` (no optimizations explicitly enabled)

### Enabling SAO on Existing Jobs

To enable SAO on an existing scheduled job:

1. **Ensure the job uses Fusion runtime:**
   ```yaml
   dbt_version: "latest-fusion"
   ```

2. **Add the SAO configuration (choose one):**

   **Option A - Recommended:**
   ```yaml
   cost_optimization_features:
     - state_aware_orchestration
   ```

   **Option B - Legacy (deprecated):**
   ```yaml
   force_node_selection: false
   ```

### Example: Before and After

**Before (no SAO):**
```yaml
jobs:
  daily_build:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    dbt_version: "1.8.0"
    name: "Daily Build"
    execute_steps:
      - dbt build
    schedule:
      cron: "0 6 * * *"
    triggers:
      schedule: true
    # ... other fields
```

**After (SAO enabled):**
```yaml
jobs:
  daily_build:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    dbt_version: "latest-fusion"  # Changed from "1.8.0"
    name: "Daily Build"
    execute_steps:
      - dbt build
    schedule:
      cron: "0 6 * * *"
    triggers:
      schedule: true
    cost_optimization_features:   # Added
      - state_aware_orchestration
    # ... other fields
```

## Migration Guide

If you're migrating from `force_node_selection` to `cost_optimization_features`:

### Before (deprecated)

```yaml
jobs:
  my_job:
    # ... other fields ...
    dbt_version: "latest-fusion"
    force_node_selection: false
```

### After (recommended)

```yaml
jobs:
  my_job:
    # ... other fields ...
    dbt_version: "latest-fusion"
    cost_optimization_features:
      - state_aware_orchestration
```

Both configurations achieve the same result, but `cost_optimization_features` is the forward-compatible approach.

## Import Behavior

When importing jobs with `import-jobs`:

- SAO fields are only included in the exported YAML if they have non-default values
- This keeps your YAML configuration clean and readable
- Jobs with SAO enabled will include the relevant field in the export

## Examples

### Scheduled Job with SAO

```yaml
jobs:
  production_build:
    account_id: 12345
    project_id: 67890
    environment_id: 11111
    dbt_version: "latest-fusion"
    name: "Daily Production Build"
    execute_steps:
      - dbt build
    execution:
      timeout_seconds: 3600
    generate_docs: true
    run_generate_sources: true
    schedule:
      cron: "0 6 * * *"  # Daily at 6 AM
    settings:
      target_name: production
      threads: 8
    triggers:
      github_webhook: false
      git_provider_webhook: false
      schedule: true
      on_merge: false
    cost_optimization_features:
      - state_aware_orchestration
```

### CI Job (SAO not applicable)

```yaml
jobs:
  ci_check:
    account_id: 12345
    project_id: 67890
    environment_id: 22222
    dbt_version: "latest-fusion"
    name: "CI Check"
    execute_steps:
      - dbt build --select state:modified+
    # ... other fields ...
    triggers:
      github_webhook: true  # CI job
      git_provider_webhook: false
      schedule: false
      on_merge: false
    # Note: Do NOT set force_node_selection or cost_optimization_features for CI jobs
```

## Troubleshooting

### Error: "State aware orchestration is not available for CI or Merge jobs"

This error occurs when attempting to set `force_node_selection` for a CI or Merge job. The solution is to remove the `force_node_selection` field from the job configuration. dbt-jobs-as-code should handle this automatically, but if you're using a custom configuration, ensure the field is not present.

### SAO not taking effect

Verify that:

1. `dbt_version` is set to `"latest-fusion"`
2. The job is a scheduled job (not CI or Merge)
3. `cost_optimization_features` includes `"state_aware_orchestration"` (or `force_node_selection` is `false`/`null`)
