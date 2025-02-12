`dbt-jobs-as-code` is a CLI built to allow managing dbt Cloud jobs via YAML files, using commands like `plan`, `sync` and `import-jobs`.

It calls various endpoints of the dbt Cloud API to create, update and delete jobs.

## How to install `dbt-jobs-as-code`

### Installing with Python

Install from [pypi.org](https://pypi.org/p/dbt-bouncer) (we recommend using a virtual environment):

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

There is also a list of typical flows leveraging `dbt-jobs-as-code` in the [typical flows](typical_flows.md) page.

## How to contribute to `dbt-jobs-as-code`

Today, raising Feature Requests and Issues and providing feedback in [the GitHub repository](https://github.com/dbt-labs/dbt-jobs-as-code) is the best way to help improve this tool.
