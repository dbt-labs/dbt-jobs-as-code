# dbt-jobs-as-code

`dbt-jobs-as-code` is a tool built to handle dbt Cloud Jobs as well-defined YAML files

A given dbt Cloud project can use both jobs-as-code and jobs-as-ui at the same time, without any conflict.

The way we differentiate jobs defined from code from the ones defined from the UI is that the code ones have a name ending with `[[<identifier>]]`.

⚠️ Important: If you plan to use this tool but have existing jobs ending with `[[<identifier>]]` you should rename them before running any command.

## Why not Terraform

Terrraform is widely used to manage infrastructure as code. And a community member has created a [Terraform provider](https://registry.terraform.io/providers/GtheSheep/dbt-cloud/latest) that can manage dbt Cloud jobs (as well as projects, environments etc...).

Using Terraform requires some knowledge about the tool and requires managing/storing/sharing a state file, containing information about the state of the application.

With this package's approach, people don't need to learn another tool and can configure dbt Cloud using YAML, a language used across the dbt ecosystem:

- **no state file required**: the link between the YAML jobs and the dbt Cloud jobs is stored in the jobs name, in the `[[<identifier>]]` part
- **YAML**: dbt users are familiar with YAML and we created a JSON schema allowing people to verify that their YAML files are correct

## Usage

### Pre-requisites

The following environment variables are used to run the code:

- `DBT_API_KEY`: [Mandatory] The dbt Cloud API key to interact with dbt Cloud. Can be a Service Token (preferred) or the API token for a given user
- `DBT_BASE_URL`: [Optional] By default, the tool queries `https://cloud.getdbt.com`, if your dbt Cloud instance is hosted on another domain, define it in this env variable (e.g. `https://emea.dbt.com`)

### Commands

The CLI comes with a few different commands

- `python src/main.py validate <config_file.yml>`: validates that the YAML file has the correct structure
  - it is possible to run the validation offline, without doing any API call
  - or online using `--online`, in order to check that the different IDs provided are correct
- `python src/main.py plan <config_file.yml>`: returns the list of actions create/update/delete that are required to have dbt Cloud reflecting the configuration file
  - this command doesn't modify the dbt Cloud jobs
- `python src/main.py sync <config_file.yml>`: create/update/delete jobs and env vars overwrites in jobs to align dbt Cloud with the configuration file
  - ⚠️ this command will modify your dbt Cloud jobs if the current configuration is different from the YAML file
- `python src/main.py import-jobs --config <config_file.yml>` or `python src/main.py import-jobs --account-id <account-id>`: Queries dbt Cloud and provide the YAML definition for those jobs. It includes the env var overwrite at the job level if some have been defined
  - it is possible to restrict the list of dbt Cloud Job IDs by adding `... -j 101 -j 123 -j 234`
  - once the YAML has been retrieved, it is possible to copy/paste it in a local YAML file to create/update the local jobs definition.
  - to move some ui-jobs to jobs-as-code, perform the following steps:
    - run the command to import the jobs
    - copy paste the job/jobs into a YAML file
    - change the `import_` id of the job in the YML file to another unique identifier
    - rename the job in the UI to end with `[[new_job_identifier]]`
    - run a `plan` command to verify that no changes are required for the given job

### Job Configuration YAML Schema

The file `src/schemas/load_job_schema.json` is a JSON Schema file that can be used to verify that the YAML config files syntax is correct.

To use it in VSCode, install the extension `YAML` and add the following line at the top of your YAML config file (change the path if need be):

```yaml
# yaml-language-server: $schema=../src/schemas/load_job_schema.json
```

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know by opening [an issue](https://github.com/dbt-labs/dbt-jobs-as-code/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/dbt-labs/dbt-jobs-as-code/blob/HEAD/CONTRIBUTING.md)

### TODO

Add info about running the code with a github actio
