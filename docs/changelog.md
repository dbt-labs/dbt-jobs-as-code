
To see the details of all changes, head to the GitHub repo

### 1.4

- Add `--templated-fields` to `import-jobs` to add Jinja variables to the generated YAML file. This can be useful to allow users to maintain jobs in the dbt Cloud UI and set a process to automatically promote those to other environments.

### 1.3

- Add this docs site
- Add `--managed-only` flag to the `import` command to only import managed jobs
- Add `--environment-id` and `--project-id` flags to `link`, `unlink` and `deactivate-jobs` commands

### 1.2

- Automatically set the identifier when using `import-jobs` on managed jobs. This automatically links the jobs to the generated YAML file.

### 1.1

- Add the ability to mention "glob" files for the YAML config and var files.
    - i.e. `dbt-jobs-as-code plan ".dbt/jobs/*"` can be used to take into consideration all files in the `.dbt/jobs` directory.

### 1.0

- Initial release of `1.0`