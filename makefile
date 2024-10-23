test-parallel:
	poetry run pytest \
		-m 'not not_in_parallel'

test-full:
	poetry run pytest \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_jobs_as_code/ 
