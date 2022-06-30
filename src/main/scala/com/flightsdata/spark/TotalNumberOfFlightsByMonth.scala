package com.flightsdata.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, to_date}

object TotalNumberOfFlightsByMonth {
  // const
  val passengerId: String = "passengerId"
  val flightId: String = "flightId"
  val from: String = "from"
  val to: String = "to"
  val flightDate: String = "date"
  val flightMonth: String = "Month"
  val dateFormat = "yyyy-MM-dd"

  // Logger init to Error
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  // template
  final case class FlightsData(passengerId: Int, flightId: Int, from: String, to: String, date: String)

  // Entry
  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalNumberOfFlightsByMonth")
      .master("local[*]")
      .getOrCreate()

    // Log to be concise
    setupLogging()

    import spark.implicits._

    // Load each line of the source data into an Dataset
    val ds = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("data/flightData.csv")
      .as[FlightsData]

    val flightsByMonth = ds.withColumn(flightMonth, month(to_date(col(flightDate), dateFormat)))
    val distinctFlightsByMonth = flightsByMonth.select(flightMonth, flightId, from, to).distinct()
    val result = distinctFlightsByMonth.groupBy(flightMonth).count().as("Number Of Flights").orderBy(flightMonth)
    result.show()
    // would have cost impact due to repartition to a single node
    // result.coalesce(1).write.option("header",true).csv("data/output/noOfFlightsByMonth.csv")
    
    // TODO column name for the result

  }
}
