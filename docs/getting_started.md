`dbt-jobs-as-code` is a CLI built to allow managing dbt Cloud jobs via YAML files, using commands like `plan`, `sync` and `import-jobs`.

It calls various endpoints of the dbt Cloud API to create, update and delete jobs.

## How to install `dbt-jobs-as-code`

### Installing with Python

Install from [pypi.org](https://pypi.org/p/dbt-jobs-as-code) (we recommend using a virtual environment):

```shell
pip install dbt-jobs-as-code # or via any other package manager
```

Show the help:

```shell
dbt-jobs-as-code --help
```


### Running as an executable using [uv](https://github.com/astral-sh/uv)

Run `dbt-jobs-as-code` as a standalone Python executable using `uv` and `uvx`:

```shell
uvx dbt-jobs-as-code --help
```

### GitHub Actions

Run `dbt-jobs-as-code` as part of your CI pipeline:

See examples in the [typical flows](typical_flows.md) page.

## Pre-requisites

The following environment variables are used to run the code:

- `DBT_API_KEY`: [Mandatory] The dbt Cloud API key to interact with dbt Cloud. Can be a Service Token (preferred, would require the "job admin" scope) or the API token of a given user
- `DBT_BASE_URL`: [Optional] By default, the tool queries `https://cloud.getdbt.com`, if your dbt Cloud instance is hosted on another domain, define it in this env variable (e.g. `https://emea.dbt.com`)

## How to use `dbt-jobs-as-code`

`dbt-jobs-as-code` can be used in many different ways:

- it can be called directly by end users from the CLI
- it can be triggered by a CI pipeline
- it can run on a periodic basis to replicate jobs between environments etc...

The list of the different commands and the parameters they accept is available in the [CLI documentation](cli.md).

### Main commands

The main commands are `plan` and `sync`. Both commands require a YML jobs definition file as an input and will replicate the jobs defined in the file to dbt Cloud.

??? example "Examples of input YML file for `plan` and `sync`"

    The YAML file can be created by hand or automatically by using the `import-jobs` command.

    ```yaml title="jobs.yml"
    # yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/dbt_jobs_as_code/schemas/load_job_schema.json

    jobs:
      job1:
        account_id: 43791
        dbt_version: null
        deferring_job_definition_id: null
        environment_id: 134459
        execute_steps:
          - "dbt run --select model1+"
          - "dbt run --select model2+"
        execution:
          timeout_seconds: 0
        generate_docs: false
        name: "My Job 1"
        project_id: 176941
        run_generate_sources: true
        schedule:
          cron: "0 */2 * * *"
        settings:
          target_name: production
          threads: 4
        state: 1
        triggers:
          git_provider_webhook: false
          github_webhook: false
          schedule: true

      job2:
        account_id: 43791
        dbt_version: null
        deferring_job_definition_id: null
        deferring_environment_id: 43791
        environment_id: 134459
        execute_steps:
          - dbt run-operation clone_all_production_schemas
          - dbt compile
        execution:
          timeout_seconds: 0
        generate_docs: false
        name: CI/CD run
        project_id: 176941
        run_generate_sources: false
        schedule:
          cron: "0 * * * *"
        settings:
          target_name: TEST
          threads: 4
        state: 1
        triggers:
          git_provider_webhook: false
          github_webhook: true # this job runs from webhooks
          schedule: false # this doesn't run on a schedule
        custom_environment_variables:
          - DBT_ENV1: My val
          - DBT_ENV2: My val2
    ```

### Typical flows

There is also a list of typical flows leveraging `dbt-jobs-as-code` in the [typical flows](typical_flows.md) page.

This includes how to leverage the tool to import jobs to different environments, how to set up a CI pipeline to plan and sync jobs, etc...

### Advanced features

For more complex use cases, please refer to the [advanced features](advanced_config/index.md) page.

## How to contribute to `dbt-jobs-as-code`

Today, raising Feature Requests and Issues and providing feedback in [the GitHub repository](https://github.com/dbt-labs/dbt-jobs-as-code) is the best way to help improve this tool.
