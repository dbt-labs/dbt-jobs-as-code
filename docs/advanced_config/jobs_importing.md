# Advanced job importing

## Generating jobs with templated fields with `import-jobs`

`plan` and `sync` commands allow adding Jinja variables to the jobs in order to use the same YAML file for different environments (see [YAML templating](templating.md)).

While it is possible to import the jobs from dbt Cloud using the `import-jobs` command and to modify the outputted YAML by hand to add variables, there is also the ability to use a YAML file to specify variables for the different jobs.

By providing the `--templated-fields` parameter, it is possible to use a YAML file to specify the variables for specific fields of the jobs.

For example, the following YAML file:

```yaml title="templ.yml"
environment_id: {{ environment_id }}
deferring_environment_id: {{ deferring_environment_id }}
triggers.schedule: {{ env_name == 'prod' }}
```

will be used to set the `project_id`, `environment_id`, `deferring_environment_id` and `triggers.schedule` fields for all the jobs in the generated YAML file and can be called with 

```bash
dbt-jobs-as-code import-jobs --account-id 1234 --project-id 3213 --environment-id 423432 --templated-fields templ.yml --managed-only
```

The outputted YAML will look like the following:

```yaml title="jobs.yml"
# yaml-language-server: $schema=https://raw.githubusercontent.com/dbt-labs/dbt-jobs-as-code/main/src/dbt_jobs_as_code/schemas/load_job_schema.json

jobs:
  my-id:
    account_id: 1234
    project_id: 3213
    environment_id: {{ environment_id }}
    dbt_version:
    name: My job name created from the UI
    settings:
      threads: 10
      target_name: default
    execution:
      timeout_seconds: 100
    deferring_job_definition_id:
    deferring_environment_id: {{ deferring_environment_id }}
    run_generate_sources: true
    execute_steps:
      - dbt build
    generate_docs: true
    schedule:
      cron: 0 1,5 * * 0,1,2,3,4,5
    triggers:
      github_webhook: false
      git_provider_webhook: false
      schedule: {{ env_name == 'prod' }}
      on_merge: false
    description: ''
    run_compare_changes: false
    compare_changes_flags: --select state:modified
    job_type: other
    triggers_on_draft_pr: false
    job_completion_trigger_condition:
    custom_environment_variables: []
```


## Automatically promote jobs between environments

### Generating jobs YML file as part of a CI/CD process

The import command from above can also be used to automatically promote jobs between environments. In that case, as part of a CI/CD process, or on a schedule, the following command can be used to generate the YAML content and save it to a file:

```bash
dbt-jobs-as-code import-jobs --account-id 1234 --project-id 3213 --environment-id 423432 --templated-fields templ.yml --managed-only > jobs.yml
```

It would then be possible to automate the creation of PRs whenever the `jobs.yml` file is updated, meaning that some jobs would have been updated in the dbt Cloud UI. The GitHub action [Create Pull Request](https://github.com/marketplace/actions/create-pull-request) could be used to implement this flow.


Then, the `jobs.yml` file can be used to import the jobs in a different environment with the following command, like described in [YAML templating](templating.md) and in the [typical flows](../typical_flows.md#advanced-flows) page :

```bash
dbt-jobs-as-code plan jobs.yml -v qa_vars.yml --limit-projects-envs-to-yml
```

With `qa_vars.yml` being the YAML file containing the variables for the QA/staging environment.

```yaml title="qa_vars.yml"
env_name: "qa"
environment_id: 456
deferring_environment_id: # (1)!
``` 

1. The `deferring_environment_id` here is set to the null value, we could also set it to a specific ID

And `prod_vars.yml` being the YAML file containing the variables for the Prod environment.

```yaml title="prod_vars.yml"
env_name: "prod"
environment_id: 789
deferring_environment_id: 
``` 


### Defining some jobs to be imported to only specific environments

It is possible to define some jobs to be imported to only specific environments by using the `--filter` parameter for `dbt-jobs-as-code import-jobs` and by using the correct identifier for the job.

To do, so the identifier of the job should be in the format `[[envs_filter:identifier]]`. The rules are the following:

- when `--filter` is not provided, all jobs are imported
- when `--filter` is provided, the following jobs are imported
    - the jobs with an `envs_filter` that contains the filter are imported
    - the jobs with an `envs_filter` that is empty are imported
    - the jobs with an `envs_filter` equal to `*` are imported

As an example, if a job is named `My daily job [[uat:my-daily-job]]` :

- `dbt-jobs-as-code import-jobs ... --filter uat` will import the job ✅
- `dbt-jobs-as-code import-jobs ...` without a filter will import the job ✅
- `dbt-jobs-as-code import-jobs ... --filter prod` will not import the job ❌

This feature allows developers to define up to which environment jobs should be imported. If a job needs to be tested in UAT before moving to Prod, the developper can add the filter `uat` to the identifier of the job and the automated import script would use the `--filter uat` parameter.

The automated promotion process would then create different jobs YML file for each environment.

Finally, when the job can be moved to Prod, the developer can remove the filter from the identifier of the job (or replace it with `*`) and the automated import script will not use the `--filter` parameter.


!!! warning "When to use this feature"

    This feature should be used only when wanting to let people create jobs in the dbt Cloud UI in an environment and get those promoted to higher environments.
    
    When possible, it is advised to maintian the jobs YAML file manually and to include both the dbt code and the job definition in the same PR.