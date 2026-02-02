.PHONY: up down logs nuke init help postgres-start postgres-stop postgres-create-tables postgres-shell postgres-status postgres-nuke

# Colors for terminal output
GREEN := \033[0;32m
YELLOW := \033[0;33m
CYAN := \033[0;36m
NC := \033[0m

init:
	@echo "$(CYAN)Initializing project...$(NC)"
	@mkdir -p ./data/cache
	@if [ ! -f .env ]; then \
		echo "$(YELLOW)Creating .env file from .env.example...$(NC)"; \
		cp .env.example .env; \
		echo "$(GREEN).env file created!$(NC)"; \
		echo "$(YELLOW)Please review and update credentials in .env file$(NC)"; \
	else \
		echo "$(YELLOW).env file already exists, skipping...$(NC)"; \
	fi
	@echo "$(GREEN)Initialization complete!$(NC)"

up: init
	@echo "$(CYAN)Starting NYC Taxi Pipeline services...$(NC)"
	@docker-compose up -d
	@echo ""
	@echo "$(GREEN)Services started successfully!$(NC)"
	@echo ""
	@echo "$(YELLOW)Service URLs:$(NC)"
	@echo "  MinIO API:      http://localhost:9000"
	@echo "  MinIO Console:  http://localhost:9001"
	@echo "  Airflow UI:     http://localhost:8080"
	@echo ""
	@echo "$(YELLOW)MinIO Credentials:$(NC)"
	@echo "  Username: $$(grep MINIO_ROOT_USER .env | cut -d '=' -f2)"
	@echo "  Password: $$(grep MINIO_ROOT_PASSWORD .env | cut -d '=' -f2)"
	@echo ""
	@echo "$(YELLOW)Airflow Credentials:$(NC)"
	@echo "  Username: admin"
	@echo "  Password: admin"
	@echo ""
	@echo "$(YELLOW)MinIO Bucket:$(NC)"
	@echo "  nyc-taxi-pipeline"
	@echo ""
	@echo "$(CYAN)Showing logs (Ctrl+C to exit):$(NC)"
	@docker-compose logs -f

down:
	@echo "$(CYAN)Stopping NYC Taxi Pipeline services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)Services stopped$(NC)"

logs:
	@docker-compose logs -f

nuke:
	@echo "$(YELLOW)WARNING: This will remove ALL containers, images, volumes, and data!$(NC)"
	@echo "$(YELLOW)This includes:$(NC)"
	@echo "  - All Docker containers (MinIO, PostgreSQL, ETL, Airflow)"
	@echo "  - All Docker images (MinIO, PostgreSQL, Airflow, etc.)"
	@echo "  - All Docker volumes (MinIO data, PostgreSQL data, Airflow logs)"
	@echo "  - All Docker networks"
	@echo "  - All build cache"
	@echo ""
	@echo "$(YELLOW)Press Ctrl+C within 5 seconds to cancel...$(NC)"
	@sleep 5
	@echo ""
	@echo "$(CYAN)Stopping and removing all containers and volumes...$(NC)"
	@docker-compose down -v --remove-orphans
	@echo ""
	@echo "$(CYAN)Removing project Docker images...$(NC)"
	@docker rmi minio/minio:latest minio/mc:latest postgres:15-alpine 2>/dev/null || true
	@echo ""
	@echo "$(CYAN)Removing dangling images...$(NC)"
	@docker image prune -af
	@echo ""
	@echo "$(CYAN)Removing all unused volumes...$(NC)"
	@docker volume prune -af
	@echo ""
	@echo "$(CYAN)Removing all unused networks...$(NC)"
	@docker network prune -f
	@echo ""
	@echo "$(CYAN)Removing build cache...$(NC)"
	@docker builder prune -af
	@echo ""
	@echo "$(CYAN)Final system cleanup...$(NC)"
	@docker system prune -af --volumes
	@echo ""
	@echo "$(GREEN)Complete cleanup finished!$(NC)"
	@echo ""
	@echo "$(CYAN)Docker system status:$(NC)"
	@docker system df

postgres-start: init
	@echo "$(CYAN)Starting PostgreSQL...$(NC)"
	@docker-compose up -d postgres
	@echo ""
	@echo "$(GREEN)PostgreSQL started!$(NC)"
	@echo "$(YELLOW)Connection details:$(NC)"
	@echo "  Host:     localhost"
	@echo "  Port:     5432"
	@echo "  Database: $$(grep POSTGRES_DB .env | cut -d '=' -f2)"
	@echo "  User:     $$(grep POSTGRES_USER .env | cut -d '=' -f2)"
	@echo ""
	@echo "$(CYAN)Waiting for PostgreSQL to be ready...$(NC)"
	@sleep 3
	@docker-compose exec postgres pg_isready -U postgres || echo "$(YELLOW)PostgreSQL is starting up...$(NC)"

postgres-stop:
	@echo "$(CYAN)Stopping PostgreSQL...$(NC)"
	@docker-compose stop postgres
	@echo "$(GREEN)PostgreSQL stopped$(NC)"

postgres-nuke: postgres-stop
	@echo "$(RED)⚠️  WARNING: This will destroy all PostgreSQL data!$(NC)"
	@echo "$(YELLOW)Press Ctrl+C to cancel, or Enter to continue...$(NC)"
	@read -r confirm
	@echo "$(CYAN)Removing PostgreSQL container and volumes...$(NC)"
	@docker-compose rm -f postgres
	@docker volume rm nyc-taxi-pipeline_postgres_data 2>/dev/null || echo "$(YELLOW)Volume already removed$(NC)"
	@echo "$(GREEN)✓ PostgreSQL completely removed$(NC)"
	@echo ""
	@echo "$(CYAN)Recreating PostgreSQL from scratch...$(NC)"
	@docker-compose up -d postgres
	@echo "$(CYAN)Waiting for PostgreSQL to initialize and run init scripts...$(NC)"
	@sleep 8
	@until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do \
		echo "$(YELLOW)  Still initializing...$(NC)"; \
		sleep 2; \
	done
	@echo "$(GREEN)✓ PostgreSQL is ready!$(NC)"
	@echo ""
	@echo "$(CYAN)Verifying database and tables...$(NC)"
	@docker-compose exec -T postgres psql -U postgres -d nyc_taxi -c "\dt taxi.*" || echo "$(RED)Tables not created yet$(NC)"
	@echo ""
	@echo "$(GREEN)✓ PostgreSQL recreated with fresh schema!$(NC)"
	@echo "$(YELLOW)Schema: taxi$(NC)"
	@echo "$(YELLOW)Tables: dim_date, dim_location, dim_payment, fact_trip$(NC)"
	@echo "$(YELLOW)All indexes and constraints created automatically$(NC)"

postgres-create-tables: postgres-start
	@echo "$(CYAN)Creating PostgreSQL tables...$(NC)"
	@docker-compose exec postgres psql -U postgres -d nyc_taxi -f /docker-entrypoint-initdb.d/create_dimensional_model.sql
	@echo "$(GREEN)Tables created successfully!$(NC)"
	@echo ""
	@echo "$(YELLOW)Tables created:$(NC)"
	@docker-compose exec postgres psql -U postgres -d nyc_taxi -c "\dt taxi.*"

postgres-shell:
	@echo "$(CYAN)Connecting to PostgreSQL...$(NC)"
	@docker-compose exec postgres psql -U postgres -d nyc_taxi

postgres-status:
	@echo "$(CYAN)PostgreSQL Status:$(NC)"
	@docker-compose exec postgres pg_isready -U postgres && echo "$(GREEN)✓ PostgreSQL is ready$(NC)" || echo "$(YELLOW)✗ PostgreSQL is not ready$(NC)"
	@echo ""
	@echo "$(CYAN)Database Info:$(NC)"
	@docker-compose exec postgres psql -U postgres -d nyc_taxi -c "SELECT version();" 2>/dev/null || echo "$(YELLOW)Cannot connect to PostgreSQL$(NC)"
	@echo ""
	@echo "$(CYAN)Table Counts:$(NC)"
	@docker-compose exec postgres psql -U postgres -d nyc_taxi -c "\
		SELECT 'dim_date' as table_name, COUNT(*) as records FROM taxi.dim_date \
		UNION ALL SELECT 'dim_location', COUNT(*) FROM taxi.dim_location \
		UNION ALL SELECT 'dim_payment', COUNT(*) FROM taxi.dim_payment \
		UNION ALL SELECT 'fact_trip', COUNT(*) FROM taxi.fact_trip;" 2>/dev/null || echo "$(YELLOW)Tables not yet created$(NC)"

help:
	@echo "$(CYAN)NYC Taxi Pipeline - Available Make commands:$(NC)"
	@echo ""
	@echo "$(YELLOW)General:$(NC)"
	@echo "  $(GREEN)make init$(NC)                     - Initialize directories and environment"
	@echo "  $(GREEN)make up$(NC)                       - Start all services (MinIO + PostgreSQL)"
	@echo "  $(GREEN)make down$(NC)                     - Stop all services"
	@echo "  $(GREEN)make logs$(NC)                     - Show service logs"
	@echo "  $(GREEN)make nuke$(NC)                     - Remove all containers, images, and volumes"
	@echo ""
	@echo "$(YELLOW)PostgreSQL:$(NC)"
	@echo "  $(GREEN)make postgres-start$(NC)           - Start PostgreSQL container"
	@echo "  $(GREEN)make postgres-stop$(NC)            - Stop PostgreSQL container"
	@echo "  $(GREEN)make postgres-create-tables$(NC)   - Create dimensional model tables"
	@echo "  $(GREEN)make postgres-shell$(NC)           - Connect to PostgreSQL shell"
	@echo "  $(GREEN)make postgres-status$(NC)          - Show PostgreSQL status and table counts"
	@echo ""
	@echo "  $(GREEN)make help$(NC)                     - Show this help message"