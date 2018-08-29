package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategy
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.kafka.common.serialization.StringDeserializer

object DetectorStreaming extends App {

  val spark = SparkSession
    .builder()
    .appName("DetectorStreaming")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .enableHiveSupport()
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  val locationStrategy = LocationStrategies.PreferConsistent

  val topics = List("test")
  val kafkaParams = Map(
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test-group",
    "auto.offset.reset" -> "earliest")
  val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)

  val kStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategy)
  
  kStream.map(_.value()).print()
  ssc.start()
  ssc.awaitTermination()

}