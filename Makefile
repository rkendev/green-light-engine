.PHONY: bootstrap test lint gate0 mvp

bootstrap:
	poetry install

test:
	poetry run pytest

lint:
	poetry run ruff check .
	poetry run isort --check .
	poetry run black --check .

gate0:
	poetry run gle-gate0

mvp:
	@echo "MVP pipeline not yet implemented. This target will later run the full chain."
