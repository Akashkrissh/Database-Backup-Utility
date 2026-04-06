.PHONY: install install-dev test lint format clean build

install:
	pip install -e .

install-dev:
	pip install -e ".[all]"
	pip install pytest pytest-cov black ruff

test:
	pytest tests/ -v --tb=short

lint:
	ruff check dbvault/

format:
	black dbvault/ tests/

clean:
	rm -rf build/ dist/ *.egg-info __pycache__ .pytest_cache

build:
	python -m build

help:
	@echo "Available commands:"
	@echo "  make install      Install dbvault"
	@echo "  make install-dev  Install with all optional deps"
	@echo "  make test         Run tests"
	@echo "  make lint         Check code style"
	@echo "  make format       Auto-format code"
	@echo "  make build        Build distribution packages"
