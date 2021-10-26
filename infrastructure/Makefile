SHELL := /bin/bash
SOURCE_VENV := source ./venv/bin/activate

init:
	python3 -m venv venv
	${SOURCE_VENV} && pip install -r requirements.txt

up:
	${SOURCE_VENV} && pulumi up
