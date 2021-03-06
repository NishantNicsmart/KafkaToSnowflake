package part8kafktosnowflake

import org.apache.spark.sql.SparkSession

object WriteSparkStreamSnowFlake  extends App {
  val spark = SparkSession.builder()
    .appName("Spark Stream")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()



  query.awaitTermination()



}
