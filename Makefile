.PHONY: test clean

VENV_PYTHON := ../.venv/Scripts/python

test:
	CONFIG_ENV=TEST $(VENV_PYTHON) -B -m pytest

test-sim:
	$(VENV_PYTHON) -m pytest app/tests/simulation_tests/sim.py


clean:
	rm -rf __pycache__ *.pyc *.pyo

run:
	$(VENV_PYTHON) -B main.py

.PHONY: start

start-api:
	uvicorn main:app --reload