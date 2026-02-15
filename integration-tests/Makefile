REPO_ROOT=$(shell cd .. && pwd)
FLIGHT_EXT=$(REPO_ROOT)/ext/flight/build/debug/extension/flight/flight.trex
SWARM_EXT=$(REPO_ROOT)/ext/swarm/build/debug/extension/swarm/swarm.trex
PGWIRE_EXT=$(REPO_ROOT)/ext/pgwire/build/debug/extension/pgwire/pgwire.trex
CIRCE_EXT=$(REPO_ROOT)/ext/circe/build/release/extension/circe/circe.trex
LLAMA_EXT=$(REPO_ROOT)/ext/llama/build/debug/extension/llama/llama.trex
CHDB_EXT=$(REPO_ROOT)/ext/chdb/build/debug/extension/chdb/chdb.trex
HANA_EXT=$(REPO_ROOT)/ext/hana/build/debug/extension/hana_scan/hana_scan.trex
TPM_EXT=$(REPO_ROOT)/ext/tpm/build/debug/extension/tpm/tpm.trex
ETL_EXT=$(REPO_ROOT)/ext/etl/build/debug/extension/etl/etl.trex
MIGRATION_EXT=$(REPO_ROOT)/ext/migration/build/debug/extension/migration/migration.trex
FHIR_EXT=$(REPO_ROOT)/ext/fhir/target/release/libfhir.so
PYTHON_BIN?=python3
VENV=./venv

.PHONY: configure test test-tier1 test-tier2 test-tier3 test-pgwire test-pgwire-swarm test-circe test-llama test-chdb test-chdb-swarm test-hana test-tpm test-etl test-etl-swarm test-fhir test-pg-trex check-extensions check-pgwire check-circe check-llama check-chdb check-hana check-tpm check-etl check-fhir check-migration check-swarm hana-up hana-down hana-clean pg-trex-up pg-trex-down clean

configure:
	$(PYTHON_BIN) -m venv $(VENV)
	$(VENV)/bin/pip install 'duckdb==1.4.4' pytest psycopg2-binary

check-extensions:
	@test -f $(FLIGHT_EXT) || (echo "Build flight first: cd ../ext/flight && make configure && make debug" && exit 1)
	@test -f $(SWARM_EXT) || (echo "Build swarm first: cd ../ext/swarm && make configure && make debug" && exit 1)

check-pgwire:
	@test -f $(PGWIRE_EXT) || (echo "Build pgwire first: cd ../ext/pgwire && make configure && make debug" && exit 1)

check-circe:
	@test -f $(CIRCE_EXT) || (echo "Build circe first: cd ../ext/circe && make configure && make release" && exit 1)

check-llama:
	@test -f $(LLAMA_EXT) || (echo "Build llama first: cd ../ext/llama && make configure && make debug" && exit 1)

check-chdb:
	@test -f $(CHDB_EXT) || (echo "Build chdb first: cd ../ext/chdb && make configure && make debug" && exit 1)

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

check-tpm:
	@test -f $(TPM_EXT) || (echo "Build tpm first: cd ../ext/tpm && make configure && make debug" && exit 1)

check-hana:
	@test -f $(HANA_EXT) || (echo "Build hana first: cd ../ext/hana && make configure && make debug" && exit 1)

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

test-tpm: check-tpm
	$(VENV)/bin/pytest -v test_tpm_standalone.py

test-hana: check-hana
	$(VENV)/bin/pytest -v test_hana_standalone.py

check-etl:
	@test -f $(ETL_EXT) || (echo "Build etl first: cd ../ext/etl && make configure && make debug" && exit 1)

test-etl: check-etl
	$(VENV)/bin/pytest -v test_etl_standalone.py

check-swarm:
	@test -f $(SWARM_EXT) || (echo "Build swarm first: cd ../ext/swarm && make configure && make debug" && exit 1)

test-etl-swarm: check-etl check-swarm
	$(VENV)/bin/pytest -v test_etl_swarm.py
pg-trex-up:
	docker compose -f docker-compose.pg_trex.yml up -d
	@echo "Waiting for pg_trex to be ready..."
	@for i in $$(seq 1 30); do \
		docker exec trex-pg-trex pg_isready -U postgres 2>/dev/null && echo "pg_trex is ready!" && exit 0; \
		sleep 2; \
	done; \
	echo "Timeout waiting for pg_trex (60s)" && exit 1

pg-trex-down:
	docker compose -f docker-compose.pg_trex.yml down -v

test-pg-trex:
	$(VENV)/bin/pytest -v test_pg_trex.py
check-migration:
	@test -f $(MIGRATION_EXT) || (echo "Build migration first: cd ../ext/migration && make configure && make debug" && exit 1)

test-migration: check-migration
	$(VENV)/bin/pytest -v test_migration_standalone.py

check-fhir:
	@test -f $(FHIR_EXT) || (echo "Build fhir first: cd ../ext/fhir && cargo build --release" && exit 1)

test-fhir: check-fhir
	$(VENV)/bin/pytest -v test_fhir_standalone.py

clean:
	rm -rf venv __pycache__ .pytest_cache
