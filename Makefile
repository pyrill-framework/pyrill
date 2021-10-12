# Copyright 2020 Telefonica Investigacion y Desarrollo, S.A.U.
# Author: gopetracca
# Project: di-testing
# 
# This Makefile is used to generate different pyspark testing environments.
# Current pyspark versions supported:
# 	- PYPSARK 2.2, 2.3, 2.4, 3.0

include Config.mk


# Recipes ************************************************************************************
.EXPORT_ALL_VARIABLES:

.PHONY: help docs

help:
	@echo "Recipes for ${PACKAGE_NAME} package"
	@echo
	@echo "General options"
	@echo "-----------------------------------------------------------------------"
	@echo "help:                    This help"
	@echo
	@make --quiet python-help
	@echo
	@make --quiet HELP_PREFIX="docs." docs.help


include Python.mk

# Dockers recipes **************************************

docs.%:
	@make -C docs/ HELP_PREFIX="docs." $(*)


