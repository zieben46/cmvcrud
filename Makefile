.PHONY: test clean

VENV_PYTHON := ../.venv/Scripts/python

test:
	$(VENV_PYTHON) -B -m pytest

clean:
	rm -rf __pycache__ *.pyc *.pyo

run:
	$(VENV_PYTHON) -B main.py

.PHONY: start

start:
	uvicorn api_controller:app --reload