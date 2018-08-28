package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object DetectorWithWindow extends App {
  val spark = SparkSession.builder()
    .appName("Fraud Detector")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  import spark.sqlContext.implicits._
  val financesDF = spark.read.json("Data/finances-small.json")

  financesDF.na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
    .na.fill("Unknown", Seq("Description"))
    .where($"Amount" =!= 0 || $"Description" === "Unknown")
    .selectExpr("Account.Number as AccountNumber", "Amount", "to_date(CAST(unix_timestamp(Date, 'MM/dd/yyyy') as TIMESTAMP)) as Date", "Description")
    .withColumn("RollingAverage", avg($"Amount")
      .over(Window.partitionBy($"AccountNumber")
        .orderBy($"Date").rowsBetween(-4, 0)))
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  financesDF.select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), $"Account.Number".as("AccountNumber"))
    .distinct()
    .coalesce(5)
    .write.mode(SaveMode.Overwrite).json("Output/finanaces-small-accounts")

  financesDF.select($"Account.Number".as("AccountNumber"), $"Amount", $"Description", to_date(unix_timestamp($"Date").cast("TIMESTAMP")).as("Date"))
    .groupBy($"AccountNumber")
    .agg(
      avg($"Amount").as("AverageTransaction"),
      sum($"Amount").as("TotalTransactions"),
      count($"Amount").as("NumberOfTransactions"),
      max($"Amount").as("MaximumTrasaction"),
      min($"Amount").as("MinimumTrasaction"),
      collect_set($"Description").as("UniqueTransactionDescriptions"))
    .write.mode(SaveMode.Overwrite).json("Output/finanaces-small-accounts-details")

}