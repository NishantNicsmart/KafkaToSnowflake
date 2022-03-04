package part8kafktosnowflake

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

object WriteFromKafkaToSnowflake extends App {

  val spark = SparkSession.builder()
    .appName("Spark Stream")
    .config("spark.master", "local")
    .getOrCreate()

  val inputStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "input_stream")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
  val jSchema = new StructType()
    .add("id", IntegerType)
    .add("name", StringType)
    .add("fee", IntegerType)
    .add("event_time", LongType)
  val stream = inputStream.select(from_json(col("value"), jSchema).as("col1"))
    .select(col("col1.*"))
  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
  SnowflakeConnectorUtils.enablePushdownSession(spark)
  val sfOptions = new scala.collection.mutable.HashMap[String, String]()

  sfOptions += (
    "sfURL" -> "https://nn35106.us-east-2.aws.snowflakecomputing.com",
    "sfAccount" -> "nn35106",
    "sfUser" -> "nicsmart",
    "sfPassword" -> "Tommmm!123",
    "sfDatabase" -> "spark_db",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "spark_role",
    "sfWarehouse" -> "SPARKWH2"
  )

  def writeToSnowflake (df: DataFrame, batchId: Long) = {
    df.toDF().show(false)
    df.toDF().write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "members_stage")
      .mode(SaveMode.Overwrite)
      .save()
  }

  stream.writeStream.trigger(Trigger.ProcessingTime("30 seconds"))
    .foreachBatch(writeToSnowflake _)
    .start()
    .awaitTermination()

}
