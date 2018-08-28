package com.aric.samples.sparkstreaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Detector extends App {
  val spark = SparkSession.builder()
    .appName("Fraud Detector")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.driver.memory", "2g")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val financesDF = spark.read.json("Data/finances-small.json")
  financesDF
    .na.drop("all", Seq("ID", "Account", "Amount", "Description", "Date"))
    .na.fill("Unknown", Seq("Description"))
    .where(($"Amount" =!= 0) || ($"Description" === "Unknown"))
    .selectExpr("Account.Number as AccountNumber", "Amount", "Date", "Description")
    .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")

  if (financesDF.hasColumn("_corrupt_record")) {
    financesDF.where($"_corrupt_record".isNotNull()).select($"_corrupt_record")
      .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
  }

  financesDF.select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), $"Account.Number".as("AccountNumber"))
    .distinct()
    .coalesce(5)
    .write.mode(SaveMode.Overwrite).json("Output/finanaces-small-accounts")

  financesDF.select($"Account.Number".as("AccountNumber"), $"Amount", $"Description", $"Date")
    .groupBy($"AccountNumber")
    .agg(
      avg($"Amount").as("AverageTransaction"),
      sum($"Amount").as("TotalTransactions"),
      count($"Amount").as("NumberOfTransactions"),
      max($"Amount").as("MaximumTrasaction"),
      min($"Amount").as("MinimumTrasaction"),
      collect_set($"Description").as("UniqueTransactionDescriptions"))
    .write.mode(SaveMode.Overwrite).json("Output/finanaces-small-accounts-details")
  /*

val financesDetail = spark.sql("SELECT AccountNumber, UniqueTransactionDescriptions from json.`sparkfundamentals/Output/finanaces-small-accounts-details`")
financesDetail.select($"AccountNumber",$"UniqueTransactionDescriptions", array_contains($"UniqueTransactionDescriptions","Movies").as("WentToMovie")).show(truncate=false)
financesDetail.select($"AccountNumber",$"UniqueTransactionDescriptions", array_contains($"UniqueTransactionDescriptions","Movies").as("WentToMovie")).where(!($"WentToMovie"===true)).show(truncate=false)
financesDetail.select($"AccountNumber", size($"UniqueTransactionDescriptions").as("NumberOfUniqueTrxTypes"),sort_array($"UniqueTransactionDescriptions").as("UniqTrx")).show()
financesDetail.createOrReplaceTempView("FinancesDetail")
spark.sql("SELECT * from FinancesDetail").show()


scala> spark.sql("SELECT accountnumber,size(UniqueTransactionDescriptions) from FinancesDetail").show()
+-------------+-----------------------------------+
|accountnumber|size(UniqueTransactionDescriptions)|
+-------------+-----------------------------------+
|  333-XYZ-999|                                  6|
|  987-CBA-321|                                  7|
|  456-DEF-456|                                  6|
|  123-ABC-789|                                  5|
+-------------+-----------------------------------+


scala> spark.sql("SELECT accountnumber,explode(UniqueTransactionDescriptions) from FinancesDetail").show()
+-------------+--------------------+
|accountnumber|                 col|
+-------------+--------------------+
|  333-XYZ-999|         Electronics|
|  333-XYZ-999|       Grocery Store|
|  333-XYZ-999|               Books|
|  333-XYZ-999|Some Totally Fake...|
|  333-XYZ-999|                 Gas|
|  333-XYZ-999|              Movies|
|  987-CBA-321|         Electronics|
|  987-CBA-321|       Grocery Store|
|  987-CBA-321|               Books|
|  987-CBA-321|                Park|
|  987-CBA-321|          Drug Store|
|  987-CBA-321|                 Gas|
|  987-CBA-321|              Movies|
|  456-DEF-456|         Electronics|
|  456-DEF-456|       Grocery Store|
|  456-DEF-456|               Books|
|  456-DEF-456|                Park|
|  456-DEF-456|          Drug Store|
|  456-DEF-456|                 Gas|
|  123-ABC-789|         Electronics|
+-------------+--------------------+
only showing top 20 rows


scala> spark.sql("SELECT accountnumber,size(UniqueTransactionDescriptions),sort_array(UniqueTransactionDescriptions) from FinancesDetail").show()
+-------------+-----------------------------------+-----------------------------------------------+
|accountnumber|size(UniqueTransactionDescriptions)|sort_array(UniqueTransactionDescriptions, true)|
+-------------+-----------------------------------+-----------------------------------------------+
|  333-XYZ-999|                                  6|                           [Books, Electroni...|
|  987-CBA-321|                                  7|                           [Books, Drug Stor...|
|  456-DEF-456|                                  6|                           [Books, Drug Stor...|
|  123-ABC-789|                                  5|                           [Drug Store, Elec...|
+-------------+-----------------------------------+-----------------------------------------------+


scala> spark.sql("SELECT accountnumber,size(UniqueTransactionDescriptions) size,sort_array(UniqueTransactionDescriptions) sorted from FinancesDetail").show(truncate=false)
+-------------+----+------------------------------------------------------------------------------------+
|accountnumber|size|sorted                                                                              |
+-------------+----+------------------------------------------------------------------------------------+
|333-XYZ-999  |6   |[Books, Electronics, Gas, Grocery Store, Movies, Some Totally Fake Long Description]|
|987-CBA-321  |7   |[Books, Drug Store, Electronics, Gas, Grocery Store, Movies, Park]                  |
|456-DEF-456  |6   |[Books, Drug Store, Electronics, Gas, Grocery Store, Park]                          |
|123-ABC-789  |5   |[Drug Store, Electronics, Grocery Store, Movies, Park]                              |
+-------------+----+------------------------------------------------------------------------------------+


scala> spark.sql("SELECT accountnumber,size(UniqueTransactionDescriptions) size,explode(sort_array(UniqueTransactionDescriptions)) sorted from FinancesDetail").show(truncate=false)
+-------------+----+----------------------------------+
|accountnumber|size|sorted                            |
+-------------+----+----------------------------------+
|333-XYZ-999  |6   |Books                             |
|333-XYZ-999  |6   |Electronics                       |
|333-XYZ-999  |6   |Gas                               |
|333-XYZ-999  |6   |Grocery Store                     |
|333-XYZ-999  |6   |Movies                            |
|333-XYZ-999  |6   |Some Totally Fake Long Description|
|987-CBA-321  |7   |Books                             |
|987-CBA-321  |7   |Drug Store                        |
|987-CBA-321  |7   |Electronics                       |
|987-CBA-321  |7   |Gas                               |
|987-CBA-321  |7   |Grocery Store                     |
|987-CBA-321  |7   |Movies                            |
|987-CBA-321  |7   |Park                              |
|456-DEF-456  |6   |Books                             |
|456-DEF-456  |6   |Drug Store                        |
|456-DEF-456  |6   |Electronics                       |
|456-DEF-456  |6   |Gas                               |
|456-DEF-456  |6   |Grocery Store                     |
|456-DEF-456  |6   |Park                              |
|123-ABC-789  |5   |Drug Store                        |
+-------------+----+----------------------------------+
only showing top 20 rows

scala> spark.sql("SELECT accountnumber,size(UniqueTransactionDescriptions) size,explode(UniqueTransactionDescriptions) trx from FinancesDetail").show(truncate=false)
+-------------+----+----------------------------------+
|accountnumber|size|trx                               |
+-------------+----+----------------------------------+
|333-XYZ-999  |6   |Electronics                       |
|333-XYZ-999  |6   |Grocery Store                     |
|333-XYZ-999  |6   |Books                             |
|333-XYZ-999  |6   |Some Totally Fake Long Description|
|333-XYZ-999  |6   |Gas                               |
|333-XYZ-999  |6   |Movies                            |
|987-CBA-321  |7   |Electronics                       |
|987-CBA-321  |7   |Grocery Store                     |
|987-CBA-321  |7   |Books                             |
|987-CBA-321  |7   |Park                              |
|987-CBA-321  |7   |Drug Store                        |
|987-CBA-321  |7   |Gas                               |
|987-CBA-321  |7   |Movies                            |
|456-DEF-456  |6   |Electronics                       |
|456-DEF-456  |6   |Grocery Store                     |
|456-DEF-456  |6   |Books                             |
|456-DEF-456  |6   |Park                              |
|456-DEF-456  |6   |Drug Store                        |
|456-DEF-456  |6   |Gas                               |
|123-ABC-789  |5   |Electronics                       |
+-------------+----+----------------------------------+
only showing top 20 rows


scala> spark.sql("select accountnumber,collect_set(trx) as trx_grp from (SELECT accountnumber,size(UniqueTransactionDescriptions) size,explode(UniqueTransactionDescriptions) trx from FinancesDetail) group by accountnumber").show(truncate=false)
+-------------+------------------------------------------------------------------------------------+
|accountnumber|trx_grp                                                                             |
+-------------+------------------------------------------------------------------------------------+
|456-DEF-456  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas]                          |
|333-XYZ-999  |[Electronics, Grocery Store, Books, Some Totally Fake Long Description, Gas, Movies]|
|987-CBA-321  |[Electronics, Grocery Store, Books, Park, Drug Store, Gas, Movies]                  |
|123-ABC-789  |[Electronics, Grocery Store, Park, Drug Store, Movies]                              |
+-------------+------------------------------------------------------------------------------------+




scala> val financesDetailExp = financesDetail.select($"Accountnumber",explode($"UniqueTransactionDescriptions").as("trxType"))
financesDetailExp: org.apache.spark.sql.DataFrame = [Accountnumber: string, trxType: string]

scala> financesDetail
financesDetail   financesDetailExp

scala> financesDetailExp.select($"*").show(truncate=false)
+-------------+----------------------------------+
|Accountnumber|trxType                           |
+-------------+----------------------------------+
|333-XYZ-999  |Electronics                       |
|333-XYZ-999  |Grocery Store                     |
|333-XYZ-999  |Books                             |
|333-XYZ-999  |Some Totally Fake Long Description|
|333-XYZ-999  |Gas                               |
|333-XYZ-999  |Movies                            |
|987-CBA-321  |Electronics                       |
|987-CBA-321  |Grocery Store                     |
|987-CBA-321  |Books                             |
|987-CBA-321  |Park                              |
|987-CBA-321  |Drug Store                        |
|987-CBA-321  |Gas                               |
|987-CBA-321  |Movies                            |
|456-DEF-456  |Electronics                       |
|456-DEF-456  |Grocery Store                     |
|456-DEF-456  |Books                             |
|456-DEF-456  |Park                              |
|456-DEF-456  |Drug Store                        |
|456-DEF-456  |Gas                               |
|123-ABC-789  |Electronics                       |
+-------------+----------------------------------+
only showing top 20 rows


scala> financesDetailExp.select($"*",when($"trxType"==="Electronics","Tech").when($"trxType"==="Gas","Energy").when($"trxType"==="Movies", "Entertainment").otherwise("Mics.").as("trxTypeGrp")).show(truncate=false)
+-------------+----------------------------------+-------------+
|Accountnumber|trxType                           |trxTypeGrp   |
+-------------+----------------------------------+-------------+
|333-XYZ-999  |Electronics                       |Tech         |
|333-XYZ-999  |Grocery Store                     |Mics.        |
|333-XYZ-999  |Books                             |Mics.        |
|333-XYZ-999  |Some Totally Fake Long Description|Mics.        |
|333-XYZ-999  |Gas                               |Energy       |
|333-XYZ-999  |Movies                            |Entertainment|
|987-CBA-321  |Electronics                       |Tech         |
|987-CBA-321  |Grocery Store                     |Mics.        |
|987-CBA-321  |Books                             |Mics.        |
|987-CBA-321  |Park                              |Mics.        |
|987-CBA-321  |Drug Store                        |Mics.        |
|987-CBA-321  |Gas                               |Energy       |
|987-CBA-321  |Movies                            |Entertainment|
|456-DEF-456  |Electronics                       |Tech         |
|456-DEF-456  |Grocery Store                     |Mics.        |
|456-DEF-456  |Books                             |Mics.        |
|456-DEF-456  |Park                              |Mics.        |
|456-DEF-456  |Drug Store                        |Mics.        |
|456-DEF-456  |Gas                               |Energy       |
|123-ABC-789  |Electronics                       |Tech         |
+-------------+----------------------------------+-------------+
only showing top 20 rows



scala> financesDetailExp.createOrReplaceTempView("FinancesDetailExp")

scala> spark.sql("SELECT accountNumber, trxType, case when trxType='Electronics' then 'Tech' else 'Mics' end as trxtypegrp from FinancesDetailExp").show(truncate=false)
+-------------+----------------------------------+----------+
|accountNumber|trxType                           |trxtypegrp|
+-------------+----------------------------------+----------+
|333-XYZ-999  |Electronics                       |Tech      |
|333-XYZ-999  |Grocery Store                     |Mics      |
|333-XYZ-999  |Books                             |Mics      |
|333-XYZ-999  |Some Totally Fake Long Description|Mics      |
|333-XYZ-999  |Gas                               |Mics      |
|333-XYZ-999  |Movies                            |Mics      |
|987-CBA-321  |Electronics                       |Tech      |
|987-CBA-321  |Grocery Store                     |Mics      |
|987-CBA-321  |Books                             |Mics      |
|987-CBA-321  |Park                              |Mics      |
|987-CBA-321  |Drug Store                        |Mics      |
|987-CBA-321  |Gas                               |Mics      |
|987-CBA-321  |Movies                            |Mics      |
|456-DEF-456  |Electronics                       |Tech      |
|456-DEF-456  |Grocery Store                     |Mics      |
|456-DEF-456  |Books                             |Mics      |
|456-DEF-456  |Park                              |Mics      |
|456-DEF-456  |Drug Store                        |Mics      |
|456-DEF-456  |Gas                               |Mics      |
|123-ABC-789  |Electronics                       |Tech      |
+-------------+----------------------------------+----------+
only showing top 20 rows



scala> financesDetail.select($"accountnumber",posexplode($"UniqueTransactionDescriptions").as(Seq("trxTypeOrder","trxType"))).show(truncate=false)
+-------------+------------+----------------------------------+
|accountnumber|trxTypeOrder|trxType                           |
+-------------+------------+----------------------------------+
|333-XYZ-999  |0           |Electronics                       |
|333-XYZ-999  |1           |Grocery Store                     |
|333-XYZ-999  |2           |Books                             |
|333-XYZ-999  |3           |Some Totally Fake Long Description|
|333-XYZ-999  |4           |Gas                               |
|333-XYZ-999  |5           |Movies                            |
|987-CBA-321  |0           |Electronics                       |
|987-CBA-321  |1           |Grocery Store                     |
|987-CBA-321  |2           |Books                             |
|987-CBA-321  |3           |Park                              |
|987-CBA-321  |4           |Drug Store                        |
|987-CBA-321  |5           |Gas                               |
|987-CBA-321  |6           |Movies                            |
|456-DEF-456  |0           |Electronics                       |
|456-DEF-456  |1           |Grocery Store                     |
|456-DEF-456  |2           |Books                             |
|456-DEF-456  |3           |Park                              |
|456-DEF-456  |4           |Drug Store                        |
|456-DEF-456  |5           |Gas                               |
|123-ABC-789  |0           |Electronics                       |
+-------------+------------+----------------------------------+
only showing top 20 rows


scala> spark.sql("select accountNumber, posexplode(UniqueTransactionDescriptions) as (a,b) from financesDetail").show(truncate=false)
+-------------+---+----------------------------------+
|accountNumber|a  |b                                 |
+-------------+---+----------------------------------+
|333-XYZ-999  |0  |Electronics                       |
|333-XYZ-999  |1  |Grocery Store                     |
|333-XYZ-999  |2  |Books                             |
|333-XYZ-999  |3  |Some Totally Fake Long Description|
|333-XYZ-999  |4  |Gas                               |
|333-XYZ-999  |5  |Movies                            |
|987-CBA-321  |0  |Electronics                       |
|987-CBA-321  |1  |Grocery Store                     |
|987-CBA-321  |2  |Books                             |
|987-CBA-321  |3  |Park                              |
|987-CBA-321  |4  |Drug Store                        |
|987-CBA-321  |5  |Gas                               |
|987-CBA-321  |6  |Movies                            |
|456-DEF-456  |0  |Electronics                       |
|456-DEF-456  |1  |Grocery Store                     |
|456-DEF-456  |2  |Books                             |
|456-DEF-456  |3  |Park                              |
|456-DEF-456  |4  |Drug Store                        |
|456-DEF-456  |5  |Gas                               |
|123-ABC-789  |0  |Electronics                       |
+-------------+---+----------------------------------+
only showing top 20 rows

 */

  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try
    def hasColumn(columnName: String) = Try(df(columnName)).isSuccess
  }
}