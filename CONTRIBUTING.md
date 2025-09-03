# Contributing to OutbreakBeacon

Thanks for helping improve OutbreakBeacon!

## Development Setup
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pre-commit install
pytest
```

## Pull Requests
1. Fork & create a feature branch.
2. Write tests for new behavior.
3. Run `ruff` and `pytest` locally.
4. Open a PR with a clear description and checklist.

## Coding Standards
- Python ≥ 3.10
- Type hints required
- Ruff for linting/format
- Pytest for tests
