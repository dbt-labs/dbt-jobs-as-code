# Contributing

The `dbt-jobs-as-code` project is open source software that is made possible by contributors opening issues, providing
feedback, and [contributing to the knowledge loop](https://www.getdbt.com/dbt-labs/values/). Whether you are a seasoned
open source contributor or a first-time committer, we welcome and encourage you to contribute code, documentation,
ideas, or problem statements to this project.

## About this document

There are many ways to contribute to the ongoing development of `dbt-jobs-as-code`, such as by participating in 
discussions and issues. We encourage you to first read our higher-level document: 
["Expectations for Open Source Contributors"](https://docs.getdbt.com/community/resources/oss-expectations).

The rest of this document serves as a more granular guide for contributing code changes to `dbt-jobs-as-code` (this 
repository). It is not intended as a guide for using `dbt-jobs-as-code`, and some pieces assume a level of familiarity
with Python development (poetry, pip, etc). Specific code snippets in this guide assume you are using macOS or Linux and
are comfortable with the command line.

## Getting the code
## Installing git

You will need git in order to download and modify this project's source code. On macOS, the best way to download git is 
to just install Xcode. 

### External contributors

If you are not a member of the `dbt-labs` GitHub organization, you can contribute to `dbt-jobs-as-code` by forking the 
`dbt-jobs-as-code` repository. For a detailed overview on forking, check out the 
[GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. Fork the `dbt-jobs-as-code` repository
2. Clone your fork locally
3. Check out a new branch for your proposed changes
4. Push changes to your fork
5. Open a pull request against `dbt-labs/dbt-jobs-as-code` from your forked repository

### dbt Labs contributors

If you are a member of the `dbt-labs` GitHub organization, you will have push access to the `dbt-jobs-as-code` repo. 
Rather than forking `dbt-jobs-as-code` to make your changes, just clone the repository, check out a new branch, and push
directly to that branch. Branch names should be prefixed by your GitHub username.

## Setting up an environment

There are some tools that will be helpful to you in developing locally. While this is the list relevant for 
`dbt-jobs-as-code` development, many of these tools are used commonly across open-source python projects.

### Tools

These are the tools used in `dbt-jobs-as-code` development and testing:

- [`poetry`](https://python-poetry.org/docs/) for packaging and virtual environment setup.
- [`pytest`](https://docs.pytest.org/en/latest/) to define, discover, and run tests
- [`flake8`](https://flake8.pycqa.org/en/latest/) for code linting
- [`black`](https://github.com/psf/black) for code formatting

A deep understanding of these tools in not required to effectively contribute to`dbt-jobs-as-code`, but we recommend 
checking out the attached documentation if you're interested in learning more about each one.

#### Virtual environments

We strongly recommend using virtual environments when developing code in `dbt-jobs-as-code`. We recommend creating this
environment in the root of the `dbt-jobs-as-code` repository using `poetry`. To create a new environment, run:
```sh
poetry install
poetry shell
```

This will create and activate a new Python virtual environment.


## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated
tests, as well as adding some new ones. These tests will ensure that:
- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

You can run specific tests or a group of tests using [`pytest`](https://docs.pytest.org/en/latest/) directly. 
With a virtualenv active and dev dependencies installed you can do things like:

```sh
# Run all unit tests in a file
python3 -m pytest tests/exporter/test_export.py

# Run a specific unit test
python3 -m pytest tests/exporter/test_export.py::test_export_jobs_yml
```

## Submitting a Pull Request

Code can be merged into the current development branch `main` by opening a pull request. A `dbt-jobs-as-code` maintainer 
will review your PR. They may suggest code revision for style or clarity, or request that you add unit or integration
test(s). These are good things! We believe that, with a little bit of help, anyone can contribute high-quality code.

Automated tests run via GitHub Actions. If you're a first-time contributor, all tests 
(including code checks and unit tests) will require a maintainer to approve. 

Once all tests are passing and your PR has been approved, a `dbt-jobs-as-code` maintainer will merge your changes into 
the active development branch. And that's it! Happy developing :tada:
