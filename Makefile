SHELL := /bin/bash

init:
	cd infrastructure && make init
	cd dbt && make init
