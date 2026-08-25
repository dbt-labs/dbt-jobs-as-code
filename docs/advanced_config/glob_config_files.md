The different commands that require a config file and/or a variables file as parameters (see command details [here](../cli.md)) can use [glob patterns](https://docs.python.org/3/library/glob.html) instead of just file names to match multiple files.

Those patterns are also called "Unix style pathname pattern expansion", and in a nutshell:

- `*` matches any sequence of characters, in a directory or a file name
- `**` matches any sequence of characters, including multiple directories
- `?` matches any single character

For example, to run the `plan` command on all the files stored in subdirectoris under the `jobs` directory, you can use the following command:

```bash
dbt-jobs-as-code plan "jobs/**/*.yml" # (1)!
```

1. Depending on your shell you might have to quote the pattern or not. For example, for `zsh` quoting is required as otherwise the shell will try to expand the pattern before passing it to the command.

If the provided config is a directory, we automatically search for all the `*.yml` and `*.yaml` files in this directory. This is particularly relevant for users with a shell not supporting the `*` character.

```bash
dbt-jobs-as-code plan ./jobs  # Equivalent to ./jobs/*.yml + ./jobs/*.yaml
```

All the matched files are merged into a single configuration before being compared to dbt Cloud, so jobs can be spread across several files.

!!! note "Deleting jobs by removing them from the YAML"
    Removing a job from the YAML, or emptying a `jobs` key entirely, only reconciles as a deletion in dbt Cloud when `plan`/`sync` are scoped with both `--project-id` and `--environment-id`:

    | Situation | Requires `--account-id`? | Result when scoped with `-p` + `-e` | Result without full `-p` + `-e` scoping |
    |---|---|---|---|
    | No matched file declares a `jobs` key at all (e.g. a pattern accidentally matching non-job YAML) | - | No-op - warning tells you to double-check your config pattern | Same as with scoping - this case doesn't depend on `-p`/`-e` at all |
    | Matched files declare jobs, but none for the requested `-p`/`-e` | No (sourced from the other jobs in the file) | Jobs in that project/environment get proposed for deletion | No-op - warning names the missing `-p`/`-e` flag |
    | Matched files declare an empty `jobs` key (`jobs: {}` or `jobs: []`) | Yes | Jobs in that project/environment get proposed for deletion | No-op - warning lists all three required flags (`-p`, `-e`, `--account-id`) |

    Nothing fails silently: whenever a deletion isn't reconciled, the warning explains why and, when applicable, exactly which flag(s) to add.