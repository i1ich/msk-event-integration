# Every target just calls a bash script, so on Windows (no make) run them directly,
# e.g.:  bash scripts/up.sh   bash scripts/demo.sh   bash scripts/down.sh
.PHONY: up down demo build schemas connector

up:
	bash scripts/up.sh

down:
	bash scripts/down.sh

demo:
	bash scripts/demo.sh

build:
	mvn -B verify

schemas:
	bash scripts/register-schemas.sh

connector:
	bash scripts/register-connector.sh
