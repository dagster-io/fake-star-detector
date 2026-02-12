"""
Partition definitions for fake-star-detector assets.

Partitions allow processing multiple repositories independently.
"""

from dagster import StaticPartitionsDefinition

repositories_to_analyze = [
    # Data Orchestration & Workflow Tools (Core Dagster Ecosystem)
    "dagster-io/dagster",  # Dagster itself
    "apache/airflow",  # Main competitor
    "PrefectHQ/prefect",  # Main competitor
    "temporalio/temporal",  # Workflow orchestration competitor
    "kestra-io/kestra",  # Workflow orchestration
    "kedro-org/kedro",  # Data orchestration framework
    "mage-ai/mage-ai",  # Data orchestration
    "dagworks-inc/hamilton",  # Data orchestration (Dagster ecosystem)
    # ETL/ELT Frameworks (Dagster Integrations)
    "dbt-labs/dbt-core",  # Core Dagster integration
    "dlt-hub/dlt",  # Data load tool (Dagster integration)
    "airbytehq/airbyte",  # ELT platform
    "TobikoData/sqlmesh",  # SQL transformation framework
    # Data Quality & Testing (Data Stack Tools)
    "great-expectations/great_expectations",  # Data quality (Dagster integration)
    "elementary-data/elementary",  # Data observability
    "sodadata/soda-core",  # Data quality
    # Data Observability & Cataloging (Modern Data Stack)
    "datahub-project/datahub",  # Data catalog
    "open-metadata/OpenMetadata",  # Data catalog
    "OpenLineage/OpenLineage",  # Data lineage (Dagster integration)
    # Data Warehousing & Processing (Modern Data Stack)
    "duckdb/duckdb",  # Core Dagster integration
    "pola-rs/polars",  # Data processing
    "apache/spark",  # Big data processing
    # MLOps (ML Workflow Tools)
    "mlflow/mlflow",  # ML lifecycle (often used with Dagster)
    "ray-project/ray",  # Distributed computing (Dagster integration)
    # Data Streaming (Real-time Data)
    "apache/kafka",  # Event streaming
    "MaterializeInc/materialize",  # Real-time data warehouse
]

REPOSITORY_PARTITIONS = StaticPartitionsDefinition(repositories_to_analyze)
