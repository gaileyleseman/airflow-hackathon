.PHONY: up down clean logs psql console omnidb reset status next-day \
        scrape-citizens scrape-citizens-fail \
        scrape-logins scrape-logins-fail \
        scrape ingest ingest-build transform transform-build \
        airflow-logs scrape-build

COMPOSE      = docker compose --project-directory .
COMPOSE_BASE = $(COMPOSE) -f infra/docker-compose.yml -f infra/docker-compose.superset.yml -f orchestration/docker-compose.yml

# Simulated pipeline date — read-only. Advance it with: make next-day
_COUNT        := $(shell cat .run_count 2>/dev/null || echo 0)
PIPELINE_DATE := $(shell date -d "2026-01-01 +$(_COUNT) days" +%Y-%m-%d)

up: scrape-build
	$(COMPOSE_BASE) up --build -d

down:
	$(COMPOSE_BASE) down --remove-orphans

# Like down, but also wipes all volumes (loses postgres data, minio data,
# and OmniDB saved connections). Use this for a full environment reset.
clean:
	$(COMPOSE_BASE) down -v --remove-orphans

logs:
	$(COMPOSE_BASE) logs -f

psql:
	$(COMPOSE_BASE) exec postgres psql -U hackathon -d hackathon

console:
	@echo "Minio console: http://localhost:9001 (minioadmin / minioadmin)"
	@echo "OmniDB:        http://localhost:8080 (admin / admin)"
	@echo "Superset:      http://localhost:8088 (admin / admin)"
	@echo "Airflow:       http://localhost:8081 (admin / admin)"

omnidb:
	@echo "OmniDB: http://localhost:8080"
	@echo "Default login: admin / admin"
	@echo ""
	@echo "PostgreSQL connection settings:"
	@echo "  Host:     postgres"
	@echo "  Port:     5432"
	@echo "  Database: hackathon"
	@echo "  User:     hackathon"
	@echo "  Password: hackathon"

reset:
	$(MAKE) clean && $(MAKE) up

status:
	$(COMPOSE_BASE) ps

# Advance the simulated date by one day.
next-day:
	@NEW_COUNT=$$(($(_COUNT) + 1)) && \
	echo $$NEW_COUNT > .run_count && \
	echo "==> Advanced to $$(date -d "2026-01-01 +$$NEW_COUNT days" +%Y-%m-%d)"

scrape-citizens:
	@echo "==> Scraping citizens for $(PIPELINE_DATE)"
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) scraper-citizens

scrape-citizens-fail:
	@echo "==> Scraping citizens for $(PIPELINE_DATE) (will FAIL)"
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) -e FAIL_SCRAPE=1 scraper-citizens

scrape-logins:
	@echo "==> Scraping logins for $(PIPELINE_DATE)"
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) scraper-logins

scrape-logins-fail:
	@echo "==> Scraping logins for $(PIPELINE_DATE) (will FAIL)"
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) -e FAIL_SCRAPE=1 scraper-logins

scrape:
	@echo "==> Scraping for $(PIPELINE_DATE)"
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) scraper-citizens
	$(COMPOSE_BASE) -f infra/docker-compose.scraper.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) scraper-logins

ingest:
	$(COMPOSE_BASE) -f infra/docker-compose.ingestion.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) ingestion

ingest-build:
	$(COMPOSE_BASE) -f infra/docker-compose.ingestion.yml \
		build ingestion

transform:
	$(COMPOSE_BASE) -f infra/docker-compose.transform.yml \
		run --rm -e PIPELINE_DATE=$(PIPELINE_DATE) transform

transform-build:
	$(COMPOSE_BASE) -f infra/docker-compose.transform.yml \
		build transform

airflow-logs:
	$(COMPOSE_BASE) logs -f airflow-scheduler

scrape-build:
	docker build --build-arg SCRIPT=scrape_citizens.py -t airflow-hackathon-scraper-citizens ./scraper
	docker build --build-arg SCRIPT=scrape_logins.py -t airflow-hackathon-scraper-logins ./scraper
