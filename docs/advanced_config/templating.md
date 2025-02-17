## Templating jobs YAML file

`validate`, `sync` and `plan` support templated YML jobs file since version 0.6.0

To add templated values to your jobs YAML file:

- update the jobs YAML file by setting some values as Jinja variables
    - e.g `project_id: {{ project_id }}` or `environment_id: {{ environment_id }}`
- and add the parameter `--vars-yml` (or `-v`) pointing to a YAML file containing values for your variables

The file called in `--vars-yml` needs to be a valid YAML file like the following:

```yaml title="vars_qa.yml"
project_id: 123
environment_id: 456
```

Templating also allows people to version control those YAML files and to have different files for different development layers, like:

- `dbt-jobs-as-code jobs.yml --vars-yml vars_qa.yml --limit-projects-envs-to-yml` for QA
- `dbt-jobs-as-code jobs.yml --vars-yml vars_prod.yml --limit-projects-envs-to-yml` for Prod

??? example "Example of templated jobs YAML file and variables files:"

    ```yaml title="jobs.yml"
    jobs:

        job1:
            account_id: 43791
            project_id: "{{project_id}}"
            environment_id: "{{ environment_id }}"
            dbt_version:
            name: My Job 1 with a new name
            settings:
            threads: 4
            # we can use the typical Jinja concatenation
            target_name: "{{ env_type ~ 'uction' }}"
            execution:
            timeout_seconds: 0
            deferring_environment_id:
            run_generate_sources: true
            execute_steps:
                - dbt run --select model1+
                - dbt run --select model2+
                - dbt compile
            generate_docs: false
            run_compare_changes: false
            schedule:
            cron: 0 */2 * * *
            triggers:
            github_webhook: false
            git_provider_webhook: false
            # we can put some logic to decide for true/false
            schedule: "{{ env_type == 'prod' }}"
            on_merge: false
            job_completion_trigger_condition:
            

        job2:
            account_id: 43791
            project_id: "{{project_id}}"
            environment_id: "{{environment_id}}"
            dbt_version:
            name: CI/CD run
            settings:
            threads: 4
            target_name: TEST
            deferring_environment_id:
            run_generate_sources: true
            run_compare_changes: true
            execute_steps:
                - dbt run-operation clone_all_production_schemas
                - dbt compile
            generate_docs: true
            schedule:
            cron: 0 * * * *
            triggers:
            github_webhook: true
            git_provider_webhook: false
            schedule: false
            on_merge: true
            job_type: other
            triggers_on_draft_pr: true
            job_completion_trigger_condition:
            condition:
                job_id: 123
                project_id: 234
                statuses:
                    - 10
                    - 20
            custom_environment_variables:
                - DBT_ENV1: My val
                - DBT_ENV2: My val2
    ```

    With the following corresponding variables files:

    ``` yaml title="vars_qa.yml"
    env_type: qa
    project_id: 123
    environment_id: 456
    deferring_environment_id: 789
    ```
    
    ``` yaml title="vars_prod.yml"
    env_type: prod
    project_id: 12
    environment_id: 231231
    deferring_environment_id: 33213
    ```


The example above is also available under `example_jobs_file/jobs_templated...` in the repo.

## Additional considerations

When using templates, you might also want to use the flag `--limit-projects-envs-to-yml`. This flag will make sure that only the projects and environments of the rendered YAML files will be checked to see what jobs to create/delete/update.


The tool will raise errors if:

- the jobs YAML file provided contains Jinja variables but `--vars-yml` is not provided
- the jobs YAML file provided contains Jinja variables that are not listed in the `--vars-yml` file