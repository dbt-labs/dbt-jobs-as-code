# Introduction

## Why `dbt-jobs-as-code`

`dbt-jobs-as-code` is a tool built to handle [dbt Cloud Jobs](https://docs.getdbt.com/docs/deploy/jobs) as a well-defined YAML files. Being standard YAML, it is possible to use YAML anchors to reduce duplicate configuration across jobs.

There is also a [templating capability](advanced_config/templating/) to use the same YAML file to update different dbt Cloud projects and/or environments.

A given dbt Cloud project can use both `dbt-jobs-as-code` and jobs degined in the UI at the same time, without any conflict.

The way we differentiate jobs defined from code from the ones defined from the UI is that the managed ones have a name ending with `[[<identifier>]]`.

!!! warning
    If you plan to use this tool but have existing jobs ending with `[[...]]` you should rename them before running any command.

Below is a video demonstration of how to use dbt-jobs-as-code as part of CI/CD, leveraging the new templating features.

[<img src="https://cdn.loom.com/sessions/thumbnails/7c263c560d2044cea9fc82ac8ec125ea-1719403943692-with-play.gif" width="700">](https://www.loom.com/share/7c263c560d2044cea9fc82ac8ec125ea?sid=4c2fe693-0aa5-4021-9e94-69d826f3eac5)

## Why not Terraform

Terrraform is widely used to manage infrastructure as code. And a comprehensive [Terraform provider](https://registry.terraform.io/providers/dbt-labs/dbtcloud/latest) exists for dbt Cloud, able to manage dbt Cloud jobs (as well as most of the rest of the dbt Cloud configuration like projects, environments, warehouse connections etc...).

Terraform is much more powerful but using it requires some knowledge about the tool and requires managing/storing/sharing a state file, containing information about the state of the application.

With this package's approach, people don't need to learn another tool and can configure dbt Cloud using YAML, a language used across the dbt ecosystem:

- **no state file required**: the link between the YAML jobs and the dbt Cloud jobs is stored in the jobs name, in the `[[<identifier>]]` part
- **YAML**: dbt users are familiar with YAML and we created a JSON schema allowing people to verify that their YAML files are correct
- by using filters like `--project-id`, `--environment-id` or `--limit-projects-envs-to-yml` and the templating deature, people can limit the projects and environments checked by the tool, which can be used to "promote" jobs between different dbt Cloud environments

## But why not both?

But more than being exclusive from each other, dbt-jobs-as-code and Terraform can be used together:

- with dbt-jobs-as-code being used to manage the day to day jobs (handled by the data team) 
- and Terraform being used to manage the rest of the dbt Cloud configuration and even CI jobs (handled by the platform or central team).