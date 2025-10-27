"""
Health Check Functions for DAGs
"""

import subprocess
import logging


def check_docker_health(**context) -> bool:
    """
    Pre-flight check: Ensure Docker containers are running
    
    Returns:
        True if all containers healthy, False otherwise
    """
    required_containers = [
        'lakehouse_spark_master',
        'lakehouse_postgres',
        'lakehouse_minio',
        'lakehouse_hive_metastore'
    ]
    
    logging.info("Performing Docker health check...")
    
    for container in required_containers:
        if not _is_container_running(container):
            logging.error(f"Container {container} is not running")
            return False
        logging.info(f"✓ {container} is running")
    
    logging.info("✓ All required containers are healthy")
    return True


def check_containers_health(containers: list) -> bool:
    """
    Check specific containers health
    
    Args:
        containers: List of container names to check
        
    Returns:
        True if all containers healthy
    """
    for container in containers:
        if not _is_container_running(container):
            logging.error(f"Container {container} is not running")
            return False
    return True


def _is_container_running(container_name: str) -> bool:
    """Check if a Docker container is running"""
    try:
        result = subprocess.run(
            ['docker', 'inspect', '-f', '{{.State.Running}}', container_name],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0 and result.stdout.strip().lower() == 'true'
    except Exception as e:
        logging.error(f"Failed to check container {container_name}: {str(e)}")
        return False
