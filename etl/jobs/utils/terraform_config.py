"""
Terraform Configuration Parser

Reads GCP configuration from terraform.tfvars and computes
the full project ID and bucket name using the same patterns as Terraform locals.
"""

import os
import re
from pathlib import Path
from typing import Optional, Tuple


def parse_tfvars(tfvars_path: Optional[str] = None) -> dict:
    """
    Parse terraform.tfvars file and return variables as a dictionary.
    
    Args:
        tfvars_path: Path to terraform.tfvars file. If None, searches for it
                     relative to the project root.
    
    Returns:
        Dictionary of variable names to values.
    """
    if tfvars_path is None:
        # Try to find terraform.tfvars relative to this file or project root
        current_dir = Path(__file__).resolve().parent
        # Go up to project root (etl/jobs/utils -> project root)
        project_root = current_dir.parent.parent.parent
        tfvars_path = project_root / "terraform" / "terraform.tfvars"
    else:
        tfvars_path = Path(tfvars_path)
    
    if not tfvars_path.exists():
        raise FileNotFoundError(f"terraform.tfvars not found at {tfvars_path}")
    
    variables = {}
    # Pattern to match: variable_name = "value" or variable_name = value
    pattern = re.compile(r'^(\S+)\s*=\s*"?([^"#\n]+)"?\s*(?:#.*)?$')
    
    with open(tfvars_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            match = pattern.match(line)
            if match:
                key = match.group(1).strip()
                value = match.group(2).strip().strip('"')
                variables[key] = value
    
    return variables


def get_gcp_config(tfvars_path: Optional[str] = None) -> Tuple[str, str]:
    """
    Get GCP_PROJECT_ID and GCS_BUCKET computed from terraform.tfvars.

    Uses the same patterns as Terraform locals (variables.tf):
    - full_project_id: ${project_id_base}-${environment}-${region}-${instance_number}
    - full_bucket_id: ${project_id_base}-${environment}-${resource-type}-${region}-${bucket_suffix}

    Args:
        tfvars_path: Optional path to terraform.tfvars file.

    Returns:
        Tuple of (project_id, bucket_name)

    Raises:
        FileNotFoundError: If terraform.tfvars not found.
        KeyError: If required variables are missing.
    """
    tfvars = parse_tfvars(tfvars_path)

    required_vars = [
        'project_id_base', 'environment', 'region',
        'instance_number', 'bucket_suffix', 'resource-type'
    ]
    missing = [v for v in required_vars if v not in tfvars]
    if missing:
        raise KeyError(f"Missing required variables in terraform.tfvars: {missing}")

    # Compute using same patterns as Terraform locals (variables.tf:91-95)
    project_id = (
        f"{tfvars['project_id_base']}-"
        f"{tfvars['environment']}-"
        f"{tfvars['region']}-"
        f"{tfvars['instance_number']}"
    )

    # Uses resource-type variable (typically 'gcs') from terraform.tfvars
    bucket_name = (
        f"{tfvars['project_id_base']}-"
        f"{tfvars['environment']}-"
        f"{tfvars['resource-type']}-"
        f"{tfvars['region']}-"
        f"{tfvars['bucket_suffix']}"
    )

    return project_id, bucket_name


def get_gcp_config_with_fallback() -> Tuple[str, str]:
    """
    Get GCP config with fallback chain:
    1. Environment variables (GCP_PROJECT_ID, GCS_BUCKET)
    2. terraform.tfvars parsing
    
    Returns:
        Tuple of (project_id, bucket_name)
    """
    project_id = os.getenv("GCP_PROJECT_ID", "")
    bucket_name = os.getenv("GCS_BUCKET", "")
    
    # If both are set, use them
    if project_id and bucket_name:
        return project_id, bucket_name
    
    # Try to parse from terraform.tfvars
    try:
        tf_project_id, tf_bucket_name = get_gcp_config()
        return (
            project_id or tf_project_id,
            bucket_name or tf_bucket_name
        )
    except (FileNotFoundError, KeyError):
        # Return whatever we have (may be empty)
        return project_id, bucket_name
