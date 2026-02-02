# GCP Configuration
project_id         = "nyc-taxi-pipeline"
project_name       = "NYC Taxi Pipeline"
billing_account_id = "0181F2-62C36F-DC7544"
region             = "us-central1"
zone               = "us-central1-a"

# MinIO Configuration
# These values should be provided via GitHub Secrets or environment variables
minio_root_user     = "minioadmin"  # Set via TF_VAR_minio_root_user
minio_root_password = "minioadmin"  # Set via TF_VAR_minio_root_password
minio_endpoint      = "minio:9000"
minio_access_key    = "minioadmin"  # Set via TF_VAR_minio_access_key
minio_secret_key    = "minioadmin"  # Set via TF_VAR_minio_secret_key

# PostgreSQL Configuration
postgres_user     = "postgres"  # Set via TF_VAR_postgres_user
postgres_password = "postgres"  # Set via TF_VAR_postgres_password
postgres_db       = "nyc_taxi"
postgres_url      = "jdbc:postgresql://localhost:5432/nyc_taxi"

# Airflow Configuration
airflow_admin_username  = "admin"
airflow_admin_password  = "admin"
airflow_admin_firstname = "Admin"
airflow_admin_lastname  = "User"
airflow_admin_email     = "admin@example.com"
airflow_admin_role      = "Admin"
