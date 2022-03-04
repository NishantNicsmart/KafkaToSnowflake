package part8kafktosnowflake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WriteToSparkKafka extends App {

  val spark = SparkSession.builder()
    .appName("Spark Stream")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  var df = Seq[(Int, String, Int)](
    (6, "Tom", 1000)
)
    .toDF("id", "name", "fee")

  df = df.withColumn("event_time", lit(System.currentTimeMillis()))
  df.toJSON.selectExpr("CAST(value AS STRING)")
    .write.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "input_stream")
    .save()

}
