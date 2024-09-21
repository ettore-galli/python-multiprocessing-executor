
install:
	pip install .
	pip install ."[format]"
	pip install ."[lint]"
	pip install ."[test]"
	pip install ."[build]"

lint:
	black multiprocessing_executor/ tests/ demo/
	ruff check multiprocessing_executor/ tests/ demo/
	mypy multiprocessing_executor/ tests/ demo/

test:
	pytest tests/ demo/

all: lint test

build-project:
	ci/build-project
	