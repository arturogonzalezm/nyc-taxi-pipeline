#!/bin/bash
# Script to set Airflow admin password to 'admin'

echo '{"admin": {"password": "admin", "role": "admin"}}' > /opt/airflow/simple_auth_manager_passwords.json.generated
echo "Admin password set to: admin"
