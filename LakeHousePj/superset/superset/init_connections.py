"""
Superset Database Connection Initialization Script
Automatically creates connections to Hive, PostgreSQL, and other data sources
Run after Superset is started: docker exec lakehouse_superset python /app/init_connections.py
"""

import sys
import os
sys.path.insert(0, '/app')

from superset.extensions import db
from superset.models.core import Database
from flask import Flask
from superset.app import create_app


def create_database_connections():
    """Create default database connections for Lakehouse."""

    app = create_app()

    with app.app_context():
        # Database 1: PostgreSQL Metastore
        print("Creating PostgreSQL connection...")
        postgres_db = Database.query.filter_by(database_name="PostgreSQL Lakehouse").first()
        if not postgres_db:
            postgres_db = Database(
                database_name="PostgreSQL Lakehouse",
                sqlalchemy_uri="postgresql://lakehouse_user:lakehouse_pass@postgres:5432/metastore_db",
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_cvas=False,
            )
            db.session.add(postgres_db)
            print("✓ PostgreSQL connection created")
        else:
            print("✓ PostgreSQL connection already exists")

        # Database 2: Hive Metastore
        print("Creating Hive Metastore connection...")
        hive_db = Database.query.filter_by(database_name="Hive Lakehouse").first()
        if not hive_db:
            hive_db = Database(
                database_name="Hive Lakehouse",
                sqlalchemy_uri="hive://hive-metastore:9083/default",
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_cvas=False,
                extra={
                    "metadata_params": {},
                    "engine_params": {
                        "hive_conf": {
                            "hive.exec.parallel": "true",
                        }
                    }
                }
            )
            db.session.add(hive_db)
            print("✓ Hive Metastore connection created")
        else:
            print("✓ Hive Metastore connection already exists")

        # Database 3: Spark SQL (if available)
        print("Creating Spark SQL connection...")
        spark_db = Database.query.filter_by(database_name="Spark SQL").first()
        if not spark_db:
            try:
                spark_db = Database(
                    database_name="Spark SQL",
                    sqlalchemy_uri="spark+thrift://spark-master:10000/default",
                    expose_in_sqllab=True,
                    allow_ctas=False,
                    allow_cvas=False,
                )
                db.session.add(spark_db)
                print("✓ Spark SQL connection created")
            except Exception as e:
                print(f"⚠ Spark SQL connection failed (optional): {str(e)}")

        # Commit all changes
        try:
            db.session.commit()
            print("\n✓ All database connections initialized successfully!")
            return True
        except Exception as e:
            db.session.rollback()
            print(f"\n✗ Error committing database connections: {str(e)}")
            return False


if __name__ == "__main__":
    success = create_database_connections()
    sys.exit(0 if success else 1)
