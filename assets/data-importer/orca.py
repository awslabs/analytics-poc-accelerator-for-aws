import sys

from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    Usage: orca <table-name> [target-file-size-in-mb]
    """
    if len(sys.argv) < 2:
        print("Usage: orca <table-name> [target-file-size-in-mb]")
        sys.exit(-1)
    table_name = sys.argv[1]
    target_file_size = int(sys.argv[2]) if len(sys.argv) > 2 else 256

    spark = SparkSession.builder.appName("IcebergOptimizer").getOrCreate()
    compact_query = f"CALL dev.system.rewrite_data_files(table => '{table_name}', options => map('target-file-size-bytes', {target_file_size * 1024 * 1024}))"
    out = spark.sql(compact_query)
    out.show()

# emr run \
#   --entry-point orca.py \
#   --application-id 00f8tl5g7gcvd80l \
#   --job-role ${JOB_ROLE_ARN} \
#   --s3-code-uri s3://${S3_BUCKET}/code/icb-import/ \
#   --s3-logs-uri s3://${S3_BUCKET}/logs/ \
#   --build \
#   --show-stdout \
#   --spark-submit-opts " --jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.sql.catalog.dev.warehouse=s3://${S3_BUCKET}/output/poc/iceberg/ --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.dev=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.dev.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory" \
#   --job-args "default.noaa_berg"