"""
spark_engine.py — OPTIONAL scale-out execution engine (Apache Spark).

WHY THIS EXISTS (and why it is not the default)
------------------------------------------------
The default engine (etl/engine.py, using delta-rs + DuckDB) is single-node and
handles tens of GB efficiently with zero cluster to operate. Most companies
loading a handful of operational DBs into BI never outgrow it, so defaulting to
Spark would be over-engineering — JVM, cluster provisioning and tuning you don't
need.

Use THIS engine when the workload genuinely justifies horizontal scale:
  * source tables in the hundreds of GB to TB range,
  * heavy transforms/joins that exceed one machine's memory,
  * you already run a Spark/Databricks/EMR cluster.

The interface mirrors engine.py (run_ingest / run_model) and writes to the SAME
Delta tables, so the storage format, primary keys and idempotency guarantees are
identical — only the execution engine changes. That is the point of the seam:
scale up without changing the storage contract or the BI layer.

NOT EXECUTED in this repo's test suite: it requires a JVM + Spark + delta-spark
runtime. It is provided as the production scale-out path, deployed on a cluster.

Requirements (on the cluster):
    pip install pyspark==3.5.* delta-spark==3.2.*
"""
from __future__ import annotations

import logging

from etl.registry import IngestSpec, ModelSpec, load_specs
from etl.settings import gold_path, resolve_dsn, resolve_path, silver_path

logger = logging.getLogger(__name__)


def _spark():
    """Build a Spark session configured for Delta Lake."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("etl-row-to-delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_ingest(spec: IngestSpec) -> int:
    """Source -> silver Delta, using Spark for distributed read + MERGE."""
    from delta.tables import DeltaTable as SparkDeltaTable

    spark = _spark()
    target = silver_path(spec.target["table"])

    # 1) Distributed extract with cursor pushdown (JDBC predicate for SQL sources)
    if spec.source["type"] in {"postgres", "mysql", "sqlite"}:
        dsn = resolve_dsn(spec.source["dsn_env"])  # convert to JDBC URL on cluster
        reader = spark.read.format("jdbc").option("url", _to_jdbc(dsn)).option(
            "dbtable", spec.source["table"]
        )
        if spec.cursor and SparkDeltaTable.isDeltaTable(spark, target):
            wm = spark.read.format("delta").load(target).agg(
                {spec.cursor: "max"}
            ).collect()[0][0]
            reader = reader.option(
                "dbtable",
                f"(SELECT * FROM {spec.source['table']} "
                f"WHERE {spec.cursor} >= '{wm}') AS t",
            )
        sdf = reader.load()
    else:  # csv
        sdf = spark.read.option("header", True).csv(resolve_path(spec.source["path_env"]))

    # 2) Transforms would be applied here as Spark SQL / DataFrame ops.
    #    (The pandas transform registry is single-node; for Spark you register
    #     equivalent DataFrame transforms — same names, distributed implementation.)

    # 3) Idempotent MERGE into Delta (or first-time create)
    if not SparkDeltaTable.isDeltaTable(spark, target):
        sdf.write.format("delta").save(target)
    else:
        pk = spec.primary_key
        (
            SparkDeltaTable.forPath(spark, target)
            .alias("t")
            .merge(sdf.alias("s"), f"t.{pk} = s.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    return sdf.count()


def run_model(spec: ModelSpec) -> int:
    """SQL over silver Delta -> gold Delta, distributed."""
    spark = _spark()
    for name in spec.inputs:
        spark.read.format("delta").load(silver_path(name)).createOrReplaceTempView(name)
    result = spark.sql(spec.sql)
    result.write.format("delta").mode("overwrite").save(gold_path(spec.target["table"]))
    return result.count()


def run_by_name(name: str) -> int:
    spec = load_specs()[name]
    if isinstance(spec, IngestSpec):
        return run_ingest(spec)
    return run_model(spec)


def _to_jdbc(sqlalchemy_dsn: str) -> str:
    """Map a SQLAlchemy DSN to a JDBC URL (postgresql://.. -> jdbc:postgresql://..)."""
    scheme = sqlalchemy_dsn.split("://", 1)[0].split("+", 1)[0]
    rest = sqlalchemy_dsn.split("://", 1)[1]
    return f"jdbc:{scheme}://{rest}"
