package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, to_date}

object TotalNumberOfFlightsByMonth {

  def main(args: Array[String]): Unit = {
    // Log to be concise
    setupLogging()

    val totalNoOfFlightsByMonth = process("data/flightData.csv", "local[*]")
    totalNoOfFlightsByMonth.show()

    // only for this exercise - writing to a file
    // would have cost impact due to repartition to a single node
    writeToAFile(totalNoOfFlightsByMonth, "data/output/noOfFlightsByMonth.csv")
  }

  def process(flightDataFilePath: String,  masterURL: String) = {
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalNumberOfFlightsByMonth")
      .master(masterURL)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Load each line of the source data into an Dataset
    val ds = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

    val flightsByMonth = ds.withColumn(flightMonth, month(to_date(col(flightDate), dateFormat)))
    val distinctFlightsByMonth = flightsByMonth.select(flightMonth, flightId, from, to).distinct()
    // Write to the Console
     distinctFlightsByMonth
      .groupBy(flightMonth)
      .count()
      .orderBy(flightMonth)
      .withColumnRenamed("count", outputColumn_NoOfFlights)
  }
}
