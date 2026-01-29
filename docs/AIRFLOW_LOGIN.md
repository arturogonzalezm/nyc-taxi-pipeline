# Airflow Login Instructions

## Credentials

```
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin
```

## Access Airflow UI

1. **Start the services**:
   ```bash
   make up
   ```

2. **Open browser**: http://localhost:8080

3. **Login with**:
   - **Username**: `admin`
   - **Password**: `admin`

## Troubleshooting

If you get "401 Unauthorized" or "Invalid credentials":

1. **Clear browser cache and cookies** for localhost:8080
2. **Try incognito/private browsing mode**
3. **Restart Airflow** if needed:
   ```bash
   docker-compose restart airflow-webserver
   ```

## Alternative: Use Airflow CLI

You can trigger DAGs via CLI without the UI:

```bash
# List all DAGs
docker exec nyc-taxi-airflow-webserver airflow dags list

# Trigger the ingestion DAG with parameters
docker exec nyc-taxi-airflow-webserver airflow dags trigger nyc_taxi_ingestion \
  --conf '{"year": 2024, "month": 1, "taxi_types": ["yellow", "green"]}'

# Check DAG run status
docker exec nyc-taxi-airflow-webserver airflow dags list-runs -d nyc_taxi_ingestion

# View task logs
docker exec nyc-taxi-airflow-webserver airflow tasks logs nyc_taxi_ingestion ingest_yellow_taxi_data <run_id>
```

## Changing Password

To change the Airflow password:

1. **Exec into container**:
   ```bash
   docker exec -it nyc-taxi-airflow-webserver bash
   ```

2. **Update password file**:
   ```bash
   echo '{"admin": "your_new_password"}' > /opt/airflow/simple_auth_manager_passwords.json.generated
   ```

3. **Restart container**:
   ```bash
   docker-compose restart airflow-webserver
   ```

## Using DAG Parameters

The `nyc_taxi_ingestion` DAG supports the following parameters:

### Via UI:
1. Go to DAGs → `nyc_taxi_ingestion`
2. Click "Trigger DAG w/ config"
3. Enter JSON parameters:
   ```json
   {
     "year": 2024,
     "month": 3,
     "taxi_types": ["yellow", "green"]
   }
   ```

### Via CLI:
```bash
docker exec nyc-taxi-airflow-webserver airflow dags trigger nyc_taxi_ingestion \
  --conf '{"year": 2024, "month": 3, "taxi_types": ["yellow"]}'
```

## Notes

- Airflow runs in **standalone mode** which includes webserver, scheduler, and triggerer in one container
- Password is stored in `/opt/airflow/simple_auth_manager_passwords.json.generated`
- Default credentials are set on container startup via the script in `scripts/set_airflow_password.sh`
- Credentials: **admin/admin** (both username and password)
