package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object JoiningDF extends App  {
  case class Person(id:Int, firstName: String, lastName: String)
  case class Roles(id: Int, Role: String)
  val spark = SparkSession.builder().appName("FunctionsFucntions").master("local[*]").enableHiveSupport().config("spark.driver.memory", "2g").getOrCreate()
  spark.createDataFrame(List(
    Person(1,"Dursun", "KOC"),
    Person(2,"Oguz", "KOCIBEY"),
    Person(3,"Elif ", "KOC"),
    Person(4,"Mustafa", "KOC")))
    .write.mode(SaveMode.Overwrite).csv("Output/joining/people")

    spark.createDataFrame(List(
    		Roles(1,"CTO"),
    		Roles(1,"Software Engineer"),
    		Roles(3,"CEO"),
    		Roles(3,"CFO"),
    		Roles(4,"Mechanical Engineer"),
    		Roles(4,"CTO")))
    .write.mode(SaveMode.Overwrite).csv("Output/joining/roles")
/*

scala> val peopleDF = spark.read.csv("sparkfundamentals/Output/joining/people").toDF("id","First","Last")
peopleDF: org.apache.spark.sql.DataFrame = [id: string, First: string ... 1 more field]

scala> peopleDF.show()
+---+-------+-------+
| id|  First|   Last|
+---+-------+-------+
|  2|   Oguz|KOCIBEY|
|  4|Mustafa|    KOC|
|  1| Dursun|    KOC|
|  3|   Elif|    KOC|
+---+-------+-------+


scala> val rolesDF = spark.read.csv("sparkfundamentals/Output/joining/roles").toDF("id","Role")
rolesDF: org.apache.spark.sql.DataFrame = [id: string, Role: string]

scala> rolesDF.show()
+---+-------------------+
| id|               Role|
+---+-------------------+
|  4|Mechanical Engineer|
|  1|  Software Engineer|
|  1|                CTO|
|  3|                CEO|
|  3|                CFO|
|  4|                CTO|
+---+-------------------+


scala> peopleDF.join(rolesDF,"Id").show()
+---+-------+----+-------------------+
| id|  First|Last|               Role|
+---+-------+----+-------------------+
|  4|Mustafa| KOC|Mechanical Engineer|
|  1| Dursun| KOC|  Software Engineer|
|  1| Dursun| KOC|                CTO|
|  3|   Elif| KOC|                CEO|
|  3|   Elif| KOC|                CFO|
|  4|Mustafa| KOC|                CTO|
+---+-------+----+-------------------+


scala> peopleDF.join(rolesDF,peopleDF("Id")===rolesDF("Id")).show()
+---+-------+----+---+-------------------+
| id|  First|Last| id|               Role|
+---+-------+----+---+-------------------+
|  4|Mustafa| KOC|  4|Mechanical Engineer|
|  1| Dursun| KOC|  1|  Software Engineer|
|  1| Dursun| KOC|  1|                CTO|
|  3|   Elif| KOC|  3|                CEO|
|  3|   Elif| KOC|  3|                CFO|
|  4|Mustafa| KOC|  4|                CTO|
+---+-------+----+---+-------------------+


scala> peopleDF.join(rolesDF,((peopleDF("Id")===rolesDF("Id")) && length(rolesDF("Role"))>4)).show()
+---+-------+----+---+-------------------+
| id|  First|Last| id|               Role|
+---+-------+----+---+-------------------+
|  4|Mustafa| KOC|  4|Mechanical Engineer|
|  1| Dursun| KOC|  1|  Software Engineer|
+---+-------+----+---+-------------------+



scala> peopleDF.join(rolesDF,Seq("Id"),"left").show()
+---+-------+-------+-------------------+
| id|  First|   Last|               Role|
+---+-------+-------+-------------------+
|  2|   Oguz|KOCIBEY|               null|
|  4|Mustafa|    KOC|                CTO|
|  4|Mustafa|    KOC|Mechanical Engineer|
|  1| Dursun|    KOC|                CTO|
|  1| Dursun|    KOC|  Software Engineer|
|  3|   Elif|    KOC|                CFO|
|  3|   Elif|    KOC|                CEO|
+---+-------+-------+-------------------+


scala> peopleDF.join(rolesDF,Seq("Id"),"right").show()
+---+-------+----+-------------------+
| id|  First|Last|               Role|
+---+-------+----+-------------------+
|  4|Mustafa| KOC|Mechanical Engineer|
|  1| Dursun| KOC|  Software Engineer|
|  1| Dursun| KOC|                CTO|
|  3|   Elif| KOC|                CEO|
|  3|   Elif| KOC|                CFO|
|  4|Mustafa| KOC|                CTO|
+---+-------+----+-------------------+


scala> peopleDF.join(rolesDF,Seq("Id"),"full").show()
+---+-------+-------+-------------------+
| id|  First|   Last|               Role|
+---+-------+-------+-------------------+
|  3|   Elif|    KOC|                CEO|
|  3|   Elif|    KOC|                CFO|
|  1| Dursun|    KOC|  Software Engineer|
|  1| Dursun|    KOC|                CTO|
|  4|Mustafa|    KOC|Mechanical Engineer|
|  4|Mustafa|    KOC|                CTO|
|  2|   Oguz|KOCIBEY|               null|
+---+-------+-------+-------------------+

scala> peopleDF.join(rolesDF,Seq("Id"),"leftsemi").show()
+---+-------+----+
| id|  First|Last|
+---+-------+----+
|  4|Mustafa| KOC|
|  1| Dursun| KOC|
|  3|   Elif| KOC|
+---+-------+----+

scala> peopleDF.join(rolesDF,Seq("Id"),"leftanti").show()
+---+-----+-------+
| id|First|   Last|
+---+-----+-------+
|  2| Oguz|KOCIBEY|
+---+-----+-------+
 */  
}