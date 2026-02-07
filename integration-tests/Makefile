REPO_ROOT=$(shell cd .. && pwd)
FLIGHT_EXT=$(REPO_ROOT)/flight/build/debug/extension/flight/flight.trex
SWARM_EXT=$(REPO_ROOT)/swarm/build/debug/extension/swarm/swarm.trex
PGWIRE_EXT=$(REPO_ROOT)/pgwire/build/debug/extension/pgwire/pgwire.trex
CIRCE_EXT=$(REPO_ROOT)/circe/build/release/extension/circe/circe.trex
LLAMA_EXT=$(REPO_ROOT)/llama/build/debug/extension/llama/llama.trex
CHDB_EXT=$(REPO_ROOT)/chdb/build/debug/extension/chdb/chdb.trex
HANA_EXT=$(REPO_ROOT)/hana/build/debug/extension/hana_scan/hana_scan.trex
PYTHON_BIN?=python3
VENV=./venv

.PHONY: configure test test-tier1 test-tier2 test-tier3 test-pgwire test-pgwire-swarm test-circe test-llama test-chdb test-chdb-swarm test-hana check-extensions check-pgwire check-circe check-llama check-chdb check-hana hana-up hana-down hana-clean clean

configure:
	$(PYTHON_BIN) -m venv $(VENV)
	$(VENV)/bin/pip install 'duckdb==1.4.0' pytest psycopg2-binary

check-extensions:
	@test -f $(FLIGHT_EXT) || (echo "Build flight first: cd ../flight && make configure && make debug" && exit 1)
	@test -f $(SWARM_EXT) || (echo "Build swarm first: cd ../swarm && make configure && make debug" && exit 1)

check-pgwire:
	@test -f $(PGWIRE_EXT) || (echo "Build pgwire first: cd ../pgwire && make configure && make debug" && exit 1)

check-circe:
	@test -f $(CIRCE_EXT) || (echo "Build circe first: cd ../circe && make configure && make release" && exit 1)

check-llama:
	@test -f $(LLAMA_EXT) || (echo "Build llama first: cd ../llama && make configure && make debug" && exit 1)

check-chdb:
	@test -f $(CHDB_EXT) || (echo "Build chdb first: cd ../chdb && make configure && make debug" && exit 1)

test: check-extensions check-pgwire
	$(VENV)/bin/pytest -v test_tier1_single_node.py test_tier2_two_node.py test_tier3_aggregation.py test_pgwire_standalone.py test_pgwire_swarm.py

test-tier1: check-extensions
	$(VENV)/bin/pytest -v test_tier1_single_node.py

test-tier2: check-extensions
	$(VENV)/bin/pytest -v test_tier2_two_node.py

test-tier3: check-extensions
	$(VENV)/bin/pytest -v test_tier3_aggregation.py

test-pgwire: check-pgwire
	$(VENV)/bin/pytest -v test_pgwire_standalone.py

test-pgwire-swarm: check-extensions check-pgwire
	$(VENV)/bin/pytest -v test_pgwire_swarm.py

test-circe: check-circe
	$(VENV)/bin/pytest -v test_circe_standalone.py

test-llama: check-llama
	$(VENV)/bin/pytest -v test_llama_standalone.py

test-chdb: check-chdb
	$(VENV)/bin/pytest -v test_chdb_standalone.py

test-chdb-swarm: check-extensions check-chdb
	$(VENV)/bin/pytest -v test_chdb_swarm.py

check-hana:
	@test -f $(HANA_EXT) || (echo "Build hana first: cd ../hana && make configure && make debug" && exit 1)

hana-up:
	docker compose -f docker-compose.hana.yml up -d
	@echo "Waiting for HANA to be ready (this may take several minutes on first start)..."
	@for i in $$(seq 1 60); do \
		docker logs trex-hana 2>&1 | grep -q "post_start" && echo "HANA is ready!" && exit 0; \
		sleep 10; \
	done; \
	echo "Timeout waiting for HANA (10 min)" && exit 1

hana-down:
	docker compose -f docker-compose.hana.yml down

hana-clean:
	docker compose -f docker-compose.hana.yml down -v

test-hana: check-hana
	$(VENV)/bin/pytest -v test_hana_standalone.py

clean:
	rm -rf venv __pycache__ .pytest_cache
