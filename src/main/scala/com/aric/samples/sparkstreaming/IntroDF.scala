package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession

object IntroDF extends App {
  val spark = SparkSession.builder().appName("IntroDF").master("local[*]").getOrCreate()
  val df = spark.createDataFrame(List(("Dursun",151),("Elif",152)))
  df.printSchema()
}