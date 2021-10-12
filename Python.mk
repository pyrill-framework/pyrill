# Copyright 2020 Telefonica Investigacion y Desarrollo, S.A.U.
# Author: Alfred
# Project: script-tmpl
# 
# This Makefile is used to generate different pyspark testing environments.
# Current pyspark versions supported:
# 	- PYPSARK 2.2, 2.3, 2.4, 3.0
PACKAGE_COVERAGE=$(PACKAGE_DIR)

PYPI_REPO?=http://artifactory.hi.inet/artifactory/api/pypi/pypi-phb-ctb
PYPI_REPO_USERNAME?=
PYPI_REPO_PASSWORD?=

ifneq "${PYPI_REPO_USERNAME}" ""
    TWINE_OPS+= --username "$(PYPI_REPO_USERNAME)" --password "$(PYPI_REPO_PASSWORD)"
else
	TWINE_OPS?=
endif

# Minimum coverage
COVER_MIN_PERCENTAGE=70

# Recipes ************************************************************************************
.PHONY: run-tests build clean beautify publish requirements all-requirements \
	flake autopep sort-imports relative-imports python-help prepush pull-request

python-help:
	@echo "Python options"
	@echo "-----------------------------------------------------------------------"
	@echo "python-help:             This help"
	@echo "run-tests:               Run tests with coverage"
	@echo "clean:                   Clean compiled files"
	@echo "flake:                   Run Flake8"
	@echo "prepush:                 Helper to run before to push to repo"
	@echo "autopep:                 Reformat code using PEP8"
	@echo "sort-imports:            Sort imports"
	@echo "beautify:                Reformat code (autopep + sort-imports)"
	@echo "build:                   Build python package"
	@echo "publish:                 Publish new version in repository"
	@echo "requirements:            Install package base requirements"
	@echo "requirements-dev:        Install development package requirements"
	@echo "requirements-lint:       Install lint requirements"
	@echo "requirements-package:    Install requirements in order to build and publish package"
	@echo "requirements-docs:       Install documentation requirements"
	@echo "all-requirements:	    Install all requirements"


# Code recipes
requirements:
	@echo "Installing base requirements..."
	python3 -m pip install -r requirements.txt

requirements-%: requirements
	@echo "Installing requirements ${*}..."
	python3 -m pip install -r requirements-${*}.txt

all-requirements: requirements requirements-lint requirements-package \
				  requirements-dev requirements-docs
	@echo "All requirements installed"


build:
	python3 setup.py bdist_wheel

publish: build
	@echo "Publishing new ${PACKAGE_NAME} version on HI Artifactory's PyPi..."
	@echo ${TWINE_OPS}
	twine upload --repository-url "${PYPI_REPO}" $(TWINE_OPS) dist/*.whl

autopep:
	autopep8 --max-line-length 120 -a -r -j 8 -i .

flake:
	@echo "Running flake8 tests..."
	flake8 ${PACKAGE_COVERAGE}
	flake8 tests
	# flake8 docs/source
	isort -c ${ISORT_PARAMS} ${PACKAGE_COVERAGE}
	isort -c ${ISORT_PARAMS} tests
	isort -c ${ISORT_PARAMS} setup.py
	# isort -c ${ISORT_PARAMS} docs/source


run-tests:
	@echo "Running unit tests..."
	nosetests -v --with-coverage --cover-min-percentage=${COVER_MIN_PERCENTAGE} --cover-package=${PACKAGE_COVERAGE} -x

prepush: flake run-tests

pull-request: flake run-tests

sort-imports:
	isort ${ISORT_PARAMS} ${PACKAGE_COVERAGE}
	isort ${ISORT_PARAMS} tests
	isort ${ISORT_PARAMS} setup.py setup_utils.py
	# isort ${ISORT_PARAMS} docs/source

relative-imports:
	absolufy-imports --never $(shell find . -name "*.py")

beautify: autopep relative-imports sort-imports

# Clean files and directories created when testing or running pyspark
clean:
	@echo "Cleaning compiled files..."
	find . | grep -E "(__pycache__|\.pyc|\.pyo|pytest_cache)$ " | xargs rm -rf
	@echo "Cleaning distribution files..."
	rm -rf dist
	@echo "Cleaning build files..."
	rm -rf build
	@echo "Cleaning egg info files..."
	rm -rf ${PACKAGE_NAME}.egg-info
	@echo "Cleaning coverage files..."
	rm -f .coverage
