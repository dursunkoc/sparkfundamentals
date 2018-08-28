package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object FunctionsFunctions extends App {
  case class Person(firstName: String, lastName: String, age: Int, weight: Option[Double], jobType: Option[String])
  val spark = SparkSession.builder().appName("FunctionsFucntions").master("local[*]").enableHiveSupport().config("spark.driver.memory", "2g").getOrCreate()
  spark.createDataFrame(List(
    Person("Dursun", "KOC", 36, Some(105.0), Some("Engineer")),
    Person("Oguz", "KOCIBEY", 22, Some(105.0), None),
    Person("Elif ", "KOC", 7, None, None),
    Person(" elif ", "KOC", 7, Some(20.0), None),
    Person("Mustafa", "KOC", 32, Some(79.0), Some("Engineer")))).write.mode(SaveMode.Overwrite).json("Output/people")
/*

scala> val peopleDF = spark.sql("SELECT * from json.`sparkfundamentals/Output/people`")
2018-08-28 01:49:31 WARN  ObjectStore:568 - Failed to get database json, returning NoSuchObjectException
peopleDF: org.apache.spark.sql.DataFrame = [age: bigint, firstName: string ... 3 more fields]

scala> peopleDF.show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
| 22|     Oguz|    null| KOCIBEY| 105.0|
|  7|    elif |    null|     KOC|  20.0|
|  7|    Elif |    null|     KOC|  null|
+---+---------+--------+--------+------+


scala> peopleDF.groupBy(trim(lower($"firstName"))).agg(first($"weight")).show()
+----------------------+--------------------+
|trim(lower(firstName))|first(weight, false)|
+----------------------+--------------------+
|                  oguz|               105.0|
|                  elif|                20.0|
|               mustafa|                79.0|
|                dursun|               105.0|
+----------------------+--------------------+


scala> peopleDF.orderBy($"weight".desc_nulls_first).groupBy(trim(lower($"firstName"))).agg(first($"weight")).show()
+----------------------+--------------------+
|trim(lower(firstName))|first(weight, false)|
+----------------------+--------------------+
|                  oguz|               105.0|
|                  elif|                null|
|               mustafa|                79.0|
|                dursun|               105.0|
+----------------------+--------------------+


scala> peopleDF.orderBy($"weight".desc_nulls_last).groupBy(trim(lower($"firstName"))).agg(first($"weight")).show()
+----------------------+--------------------+
|trim(lower(firstName))|first(weight, false)|
+----------------------+--------------------+
|                  oguz|               105.0|
|                  elif|                20.0|
|               mustafa|                79.0|
|                dursun|               105.0|
+----------------------+--------------------+


scala> peopleDF.withColumn("weight", coalesce($"weight",lit(0.0))).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
| 22|     Oguz|    null| KOCIBEY| 105.0|
|  7|    elif |    null|     KOC|  20.0|
|  7|    Elif |    null|     KOC|   0.0|
+---+---------+--------+--------+------+


scala> peopleDFCorrected.filter(locate("engineer", lower($"jobType"))>0).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+


scala> peopleDFCorrected.filter(locate("engineer", $"jobType")>0).show()
+---+---------+-------+--------+------+
|age|firstName|jobType|lastName|weight|
+---+---------+-------+--------+------+
+---+---------+-------+--------+------+

scala> peopleDFCorrected.filter(instr(lower($"jobType"),"engine")>0).select($"*",instr(lower($"jobtype"), "n")).show()
+---+---------+--------+--------+------+------------------------+
|age|firstName| jobType|lastName|weight|instr(lower(jobtype), n)|
+---+---------+--------+--------+------+------------------------+
| 36|   Dursun|Engineer|     KOC| 105.0|                       2|
| 32|  Mustafa|Engineer|     KOC|  79.0|                       2|
+---+---------+--------+--------+------+------------------------+


scala> peopleDFCorrected.filter(lower($"jobType").contains("engine")).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+

scala> peopleDFCorrected.filter(lower($"jobType").like("engine%")).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+


scala> peopleDFCorrected.filter(lower($"jobType").isin(List("engineer"):_*)).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+


scala> peopleDFCorrected.filter(lower($"jobType").isin("engineer")).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+


scala> peopleDFCorrected.filter(lower($"jobType").isin("engineer","catalyst")).show()
+---+---------+--------+--------+------+
|age|firstName| jobType|lastName|weight|
+---+---------+--------+--------+------+
| 36|   Dursun|Engineer|     KOC| 105.0|
| 32|  Mustafa|Engineer|     KOC|  79.0|
+---+---------+--------+--------+------+



scala> val df = spark.createDataFrame(List((1,"This is a"),(2,"new age for me"))).toDF("id","text")
df: org.apache.spark.sql.DataFrame = [id: int, text: string]

scala> df.show()
+---+--------------+
| id|          text|
+---+--------------+
|  1|     This is a|
|  2|new age for me|
+---+--------------+

scala> spark.udf.register("capitilize_words",(t:String)=>t.split(" ").map(_.capitalize).mkString(" "))
res75: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,StringType,Some(List(StringType)))

scala> df.selectExpr("*","capitilize_words(text)").show()
+---+--------------+--------------------------+
| id|          text|UDF:capitilize_words(text)|
+---+--------------+--------------------------+
|  1|     This is a|                 This Is A|
|  2|new age for me|            New Age For Me|
+---+--------------+--------------------------+


scala> val cap_wd=udf((text:String, delim:String)=>text.split(delim).map(_.capitalize).mkString(delim))
cap_wd: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function2>,StringType,Some(List(StringType, StringType)))

scala> df.selectExpr("*").show()
+---+--------------+
| id|          text|
+---+--------------+
|  1|     This is a|
|  2|new age for me|
+---+--------------+


scala> df.selectExpr("*").select($"*",cap_wd($"text", lit(" "))).show()
+---+--------------+--------------+
| id|          text|  UDF(text,  )|
+---+--------------+--------------+
|  1|     This is a|     This Is A|
|  2|new age for me|New Age For Me|
+---+--------------+--------------+

 */
}