SHELL := /bin/bash
SOURCE_VENV := source ./venv/bin/activate

STAGE_0 = 2020-01-04
STAGE_1 = 2020-01-05
STAGES = DBT_STAGE_0_MAX_DATE=${STAGE_0} DBT_STAGE_1_MAX_DATE=${STAGE_1}

init:
	python3 -m venv venv \
		&& ${SOURCE_VENV} \
		&& python3 -m pip install --upgrade pip \
		&& pip install -r requirements.txt \
		&& dbt deps

dbt: dbt-run dbt-test dbt-docs

dbt-test:
	${SOURCE_VENV} && ${STAGES} dbt test

dbt-run:
	${SOURCE_VENV} && ${STAGES} dbt run

dbt-docs:
	${SOURCE_VENV} && ${STAGES} dbt docs generate
