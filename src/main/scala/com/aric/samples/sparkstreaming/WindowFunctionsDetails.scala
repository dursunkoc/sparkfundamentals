package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object WindowFunctionsDetails extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("WindowFunctionsDetails")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .enableHiveSupport()
    .getOrCreate()

  val trx: List[Row] = List(
    Row(1, "Dursun", "New"),
    Row(2, "Dursun", "Open"),
    Row(3, "Yasemin", "New"),
    Row(4, "Unknown", "New"),
    Row(5, "Dursun", "Open"),
    Row(6, "Dursun", "Pending"),
    Row(7, "Yasemin", "Pending"),
    Row(8, "Yasemin", "Resolved"),
    Row(9, "Unknown", "Invalid"),
    Row(10, "Dursun", "Resolved"))

  val struct: StructType =
    StructType(
      StructField("id", IntegerType, true) ::
        StructField("customer", StringType, false) ::
        StructField("status", StringType, false) :: Nil)

  val statuses = spark.createDataFrame(spark.sparkContext.parallelize(trx), struct)
  statuses.write.json("Output/statuses")
  /*
scala> statuses.select($"customer",$"status",lag($"status",1,"N/A").over(Window.partitionBy($"customer").orderBy("id")).as("prevStatus")).show()
+--------+--------+----------+
|customer|  status|prevStatus|
+--------+--------+----------+
| Yasemin|     New|       N/A|
| Yasemin| Pending|       New|
| Yasemin|Resolved|   Pending|
| Unknown|     New|       N/A|
| Unknown| Invalid|       New|
|  Dursun|     New|       N/A|
|  Dursun|    Open|       New|
|  Dursun|    Open|      Open|
|  Dursun| Pending|      Open|
|  Dursun|Resolved|   Pending|
+--------+--------+----------+


scala> statuses.select($"customer",$"status",lag($"status",1,"N/A").over(Window.partitionBy($"customer").orderBy("id")).as("prevStatus"),row_number().over(Window.partitionBy($"customer").orderBy("id")).as("trxId")).show()
+--------+--------+----------+-----+
|customer|  status|prevStatus|trxId|
+--------+--------+----------+-----+
| Yasemin|     New|       N/A|    1|
| Yasemin| Pending|       New|    2|
| Yasemin|Resolved|   Pending|    3|
| Unknown|     New|       N/A|    1|
| Unknown| Invalid|       New|    2|
|  Dursun|     New|       N/A|    1|
|  Dursun|    Open|       New|    2|
|  Dursun|    Open|      Open|    3|
|  Dursun| Pending|      Open|    4|
|  Dursun|Resolved|   Pending|    5|
+--------+--------+----------+-----+

scala> spark.sql("SELECT customer, status, lag(status,1) over(partition by customer order by id) as prevstat, row_number() over(partition by customer order by id) as trxId from statuses").show()
+--------+--------+--------+-----+
|customer|  status|prevstat|trxId|
+--------+--------+--------+-----+
| Yasemin|     New|    null|    1|
| Yasemin| Pending|     New|    2|
| Yasemin|Resolved| Pending|    3|
| Unknown|     New|    null|    1|
| Unknown| Invalid|     New|    2|
|  Dursun|     New|    null|    1|
|  Dursun|    Open|     New|    2|
|  Dursun|    Open|    Open|    3|
|  Dursun| Pending|    Open|    4|
|  Dursun|Resolved| Pending|    5|
+--------+--------+--------+-----+
   */
}