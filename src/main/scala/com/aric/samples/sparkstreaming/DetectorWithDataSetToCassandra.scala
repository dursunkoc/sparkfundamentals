package com.aric.samples.sparkstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed
import java.sql.Date
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.cassandra._

object DetectorWithDataSetToCassandra extends App {

  case class Account(number: String, firstName: String, lastName: String)
  case class Transaction(id: Long, account: Account, amount: Double, description: String, date: Date)
  case class TransactionForAverage(accountNumber: String, amount: Double, description: String, date: Date)

  val spark = SparkSession.builder()
    .appName("DetectorWithDataSet")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.cassandra.connection.host", "localhost")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sqlContext.implicits._

  val financesDS: Dataset[Transaction] = spark
    .read
    .json("Data/finances-small.json")
    .withColumn("Date", to_date(unix_timestamp($"Date", "MM/dd/yyyy").cast("timestamp")))
    .as[Transaction]

  financesDS
    .na.drop(how = "all", cols = Seq("ID", "Account", "Amount", "Date", "Description"))
    .na.fill("Unknown", Seq("Description")).as[Transaction]
    //.filter(tx => tx.amount != 0 || tx.description == "Unknown")
    .where(($"Account.Number" =!= 0) || ($"Description" === "Unknown"))
    .withColumn("RollingAverge", avg($"Amount").over(Window.partitionBy($"Account.Number").orderBy($"Date").rowsBetween(-4, 0)))
    .select(
      $"Account.Number".as("AccountNumber").as[String],
      $"Amount".as[Double],
      $"Date".as[Date],
      $"Description".as[String],
      $"RollingAverge".as[Double])
    .write.mode(SaveMode.Overwrite).csv("Output/finances-small-ds")

  val financesSmallAccounts = financesDS.map(trx => (s"${trx.account.firstName} ${trx.account.lastName}", trx.account.number))
    .distinct().coalesce(5).toDF("FullName", "AccountNumber")

  financesSmallAccounts.write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts-ds")

  financesDS.select(
    $"Account.Number".as("accountNumber").as[String],
    $"Amount".as[Double],
    $"Description".as[String],
    $"Date".as[Date]).as[TransactionForAverage]
    .groupBy($"accountNumber")
    //.groupByKey(_.accountNumber)
    //    .agg(
    //      typed.avg[TransactionForAverage](_.amount).as("AverageTransaction").as[Double],
    //      typed.sum[TransactionForAverage](_.amount).as("SumTransaction").as[Double],
    //      typed.sum[TransactionForAverage](_.amount).as("CountOfTransaction").as[Double],
    //      max($"Amount").as("MaximumTransaction").as[Double]
    //      )
    .agg(
      avg($"Amount").as("average_transaction"),
      sum($"Amount").as("total_transactions"),
      count($"Amount").as("number_of_transactions"),
      max($"Amount").as("max_transaction"),
      min($"Amount").as("min_transaction"),
      collect_set($"Description").as("unique_transaction_description"))
    .withColumnRenamed("accountNumber", "account_number")
    .write.mode(SaveMode.Overwrite)
    //.format("org.apache.spark.sql.cassandra").options(Map("keyspace"->"finances", "table"->"account_aggregates","confirm.truncate"->"true"))
    .options(Map("confirm.truncate"->"true"))
    .cassandraFormat(table = "account_aggregates", keyspace = "finances")
    .save

}