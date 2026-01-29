.PHONY: up down logs nuke init help

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
	@echo ""
	@echo "$(YELLOW)MinIO Credentials:$(NC)"
	@echo "  Username: $$(grep MINIO_ROOT_USER .env | cut -d '=' -f2)"
	@echo "  Password: $$(grep MINIO_ROOT_PASSWORD .env | cut -d '=' -f2)"
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
	@echo "$(YELLOW)WARNING: This will remove all containers, images, and volumes!$(NC)"
	@echo "$(YELLOW)Press Ctrl+C within 5 seconds to cancel...$(NC)"
	@sleep 5
	@echo ""
	@echo "$(CYAN)Stopping and removing all containers...$(NC)"
	@docker-compose down -v
	@echo "$(CYAN)Removing Docker images...$(NC)"
	@docker rmi minio/minio:latest minio/mc:latest 2>/dev/null || true
	@echo "$(CYAN)Pruning Docker system...$(NC)"
	@docker system prune -af --volumes
	@echo ""
	@echo "$(GREEN)Cleanup complete!$(NC)"

help:
	@echo "$(CYAN)NYC Taxi Pipeline - Available Make commands:$(NC)"
	@echo ""
	@echo "  $(GREEN)make init$(NC)              - Initialize directories and environment"
	@echo "  $(GREEN)make up$(NC)                - Start all services (MinIO)"
	@echo "  $(GREEN)make down$(NC)              - Stop all services"
	@echo "  $(GREEN)make logs$(NC)              - Show service logs"
	@echo "  $(GREEN)make nuke$(NC)              - Remove all containers, images, and volumes"
	@echo "  $(GREEN)make help$(NC)              - Show this help message"
