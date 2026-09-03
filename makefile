test-parallel:
	uv run pytest \
		-m 'not not_in_parallel'

test-full:
	uv run pytest \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_jobs_as_code/

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

typecheck:
	uv run ty check src tests
