package part2dataframes

import java.sql.DriverManager

import org.apache.spark.sql.{SaveMode, SparkSession}
import part2dataframes.DataSources.{driver, password, url, user}

object WriteEmpDataFrameToSnowflake extends App {

  val spark = SparkSession.builder()
    .appName("Write to Snowflake")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val properties = new java.util.Properties()
  properties.put("user", "nicsmart")
  properties.put("password", "Tommmm!123")
  properties.put("account", "nn35106")
  properties.put("warehouse", "COMPUTE_WH")
  properties.put("db", "EMP")
  properties.put("schema", "public")
  properties.put("role", "ACCOUNTADMIN")

  //JDBC connection string
  val jdbcUrl = "jdbc:snowflake://nn35106.us-east-2.aws.snowflakecomputing.com/"
  val connection = DriverManager.getConnection(jdbcUrl, properties)
  val statement = connection.createStatement
  statement.executeUpdate("use emp")
  statement.executeUpdate("create or replace table EMPLOYEE(name VARCHAR, department VARCHAR, salary number)")
  statement.execute("insert into EMPLOYEE (name, department, salary) VALUES ('James', 'IT', 10000)")



  val simpleData = Seq(("James",  30),
    ("Michael",  46),
    ("Robert",  41),
    ("Maria",  30),
    ("Raman",  30),
    ("Scott",  33),
    ("Jen",  39),
    ("Jeff", 30),
    ("Kumar", 20)
  )
  val df = simpleData.toDF("name", "age")

  df.show()

  var sfOptions = Map(
    "sfURL" -> "https://nn35106.us-east-2.aws.snowflakecomputing.com",
    "sfAccount" -> "nn35106",
    "sfUser" -> "nicsmart",
    "sfPassword" -> "Tommmm!123",
    "sfDatabase" -> "spark_db",
    "sfSchema" -> "PUBLIC",
    "sfRole" -> "spark_role",
    "sfWarehouse" -> "COMPUTE_WH"
  )

  val source = "net.snowflake.spark.snowflake"

  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  val driver = "net.snowflake.client.jdbc.SnowflakeDriver"
  val url = "jdbc:snowflake://nn35106.us-east-2.aws.snowflakecomputing.com/"

  df.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", jdbcUrl)
    .option("account", "nn35106")
    .option("user", "nicsmart")
    .option("password", "Tommmm!123")
    .option("dbtable", "test_spark_txt")
    .option("db", "spark_db")
    .option("schema", "public")
    .option("role","spark_role")
    .option("warehouse", "sparkwh2")
    .save()

  statement.close
  connection.close()

}

