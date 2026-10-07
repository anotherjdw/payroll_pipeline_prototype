.PHONY: install test test-unit test-integration test-e2e lint format typecheck check fmt plan package clean

install:
	pip install -e ".[dev]"
	@if git rev-parse --git-dir >/dev/null 2>&1; then pre-commit install; \
	else echo "Not a git repository yet: run 'git init', then 'pre-commit install'."; fi

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration

test-e2e:
	pytest tests/end_to_end

lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

typecheck:
	mypy src/

check: lint typecheck test

fmt:
	terraform fmt -recursive infra/terraform/

plan:
	terraform -chdir=infra/terraform plan

package:
	mkdir -p lib
	rm -f lib/payroll_pipeline_prototype.zip
	cd src && find payroll_pipeline_prototype -type f \
	  -not -path '*/__pycache__/*' \
	  -not -path 'payroll_pipeline_prototype/contracts/*' \
	  -not \( -path '*/jobs/*' -name '*.yaml' \) \
	  | zip -q -X "$(CURDIR)/lib/payroll_pipeline_prototype.zip" -@

clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache .ruff_cache dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +
