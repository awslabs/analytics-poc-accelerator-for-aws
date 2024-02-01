import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream_name", type=str, help="Name of the Kinesis stream to consume")
    parser.add_argument("--stream_region", type=str, help="Region that the stream is in")
    parser.add_argument("--checkpoint_s3_uri", type=str, help="S3 location for streaming checkpoints")
    return parser.parse_args()


def kinesis_printer(spark: SparkSession, stream: str, region_name: str, checkpoint_uri: str):
    """Simply groups the stream and counts each unique record

    You can generate stream records using the following AWS CLI command:

        aws kinesis put-record \
            --stream-arn arn:aws:kinesis:<AWS_REGION>:<AWS_ACCOUNT_ID>:stream/<STREAM_NAME> \
            --data '{"name":"Damon"}' \
            --partition-key $(uuidgen) \
            --cli-binary-format raw-in-base64-out

    Args:
        spark (SparkSession): A pre-existing Spark session
        stream (str): The name of the Kinesis stream
        region_name (str): The AWS region in which the stream is running
        checkpoint_uri (str): An S3 URI in which to store streaming checkpoints (e.g. s3://bucket/prefix/)
    """
    kinesis = (
        spark.readStream.format("aws-kinesis")
        .option("kinesis.streamName", stream)
        .option("kinesis.consumerType", "GetRecords")
        .option("kinesis.endpointUrl", f"https://kinesis.{region_name}.amazonaws.com")
        .option("kinesis.region", region_name)
        .option("kinesis.startingposition", "TRIM_HORIZON")
        .load()
    )

    # Cast data into string and group by data column
    # Fails without checkpoint Location
    query = (
        kinesis.selectExpr("CAST(data AS STRING)")
        .groupBy("data")
        .count()
        .writeStream.format("console")
        .outputMode("complete")
        .option("checkpointLocation", checkpoint_uri)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    args = parse_args()
    spark = SparkSession.builder.appName("PySparkKinesis").getOrCreate()

    kinesis_printer(
        spark,
        args.stream_name,
        args.stream_region,
        args.checkpoint_s3_uri,
    )
