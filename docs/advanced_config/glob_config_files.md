The different commands that require a config file and/or a variables file as parameters (see command details [here](../cli.md)) can use [glob patterns](https://docs.python.org/3/library/glob.html) instead of just file names to match multiple files.

Those patterns are also called "Unix style pathname pattern expansion", and in a nutshell:

- `*` matches any sequence of characters, in a directory or a file name
- `**` matches any sequence of characters, including multiple directories
- `?` matches any single character

For example, to run the `plan` command on all the files stored in subdirectoris under the `jobs` directory, you can use the following command:

```bash
dbt-jobs-as-code validate --config-file jobs/**/*.yml
```