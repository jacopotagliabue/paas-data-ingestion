SHELL := /bin/bash
SOURCE_VENV := source ./venv/bin/activate

init:
	python3 -m venv venv && \
		${SOURCE_VENV} && \
		python3 -m pip install --upgrade pip \
		pip install -r requirements.txt

run:
	${SOURCE_VENV} && DBT_STAGE_0_MAX_DATE=2020-01-10 DBT_STAGE_1_MAX_DATE=2020-01-11 dbt run
