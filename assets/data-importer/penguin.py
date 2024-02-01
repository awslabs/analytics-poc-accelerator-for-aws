# import to iceberg
# - Source location
# - Source data type
# - Target table name
# - Partitions

# import to iceberg
# - Source DataFrame
# - Target table name
# - Partitions

import sys
from typing import Dict, List

from pyspark.sql import DataFrame, Row, SparkSession


def default_options_for_type(source_type: str) -> Dict:
    return {"csv": {"inferSchema": "true", "header": "true"}}.get(source_type, {})


def import_raw_to_iceberg(
    spark: SparkSession,
    source: str,
    source_type: str,
    target_table: str,
    partitions: List[str],
) -> DataFrame:
    kwargs = default_options_for_type(source_type)
    # Create our DataFrame using the desired parameters
    df = spark.read.format(source_type).load(source, **kwargs)

    # sortWithinPartitions is required when using Iceberg auto-partitioning
    df = df.sortWithinPartitions("DATE")

    # Create a temp table we can use to create our Iceberg table
    df.createOrReplaceTempView("input_table")

    # Create our Iceberg table
    # Requires glue:UpdateTable
    query = f"""
        CREATE OR REPLACE TABLE dev.{target_table}
        USING iceberg
        PARTITIONED BY ({','.join(partitions)})
        AS SELECT * FROM input_table
    """
    spark.sql(query)


if __name__ == "__main__":
    """
    Usage: penguin <s3_source> <source_type> <target_table> <partitions>
    """
    # sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    s3_source = "s3://noaa-gsod-pds/20*/72793524234.csv"
    source_type = "csv"
    target_table = "poc_default.noaa_berg"
    partitions = ["years(DATE)"]
    if len(sys.argv) > 1:
        s3_source = sys.argv[1]
        source_type = sys.argv[2]
        target_table = sys.argv[3]
        partitions = sys.argv[4].split(",")

    spark = SparkSession.builder.appName("IcebergImporter").getOrCreate()
    import_raw_to_iceberg(spark, s3_source, source_type, target_table, partitions)
    count = spark.sql(f"SELECT count(*) FROM dev.{target_table}").first()[0]
    print(f"Number of records in iceberg: {count}")

# emr run \
#   --entry-point penguin.py \
#   --application-id 00f8tl5g7gcvd80l \
#   --job-role $JOB_ROLE_ARN \
#   --s3-code-uri s3://${S3_BUCKET}/code/icb-import/ \
#   --build \
#   --s3-logs-uri s3://${S3_BUCKET}/logs/ \
#   --show-stdout \
#   --spark-submit-opts " --jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.catalog.dev.warehouse=s3://${S3_BUCKET}/output/poc/iceberg/ --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory" \
#   --job-args "s3://noaa-gsod-pds/20*/72793524234.csv,csv,poc_default.noaa_berg_seattle,years(DATE)"
#   --job-args "s3://noaa-gsod-pds/*/*.csv,csv,poc_default.noaa_berg,years(DATE)"
