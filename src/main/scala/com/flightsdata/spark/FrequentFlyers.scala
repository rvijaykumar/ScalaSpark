package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object FrequentFlyers {

  def main(args: Array[String]): Unit = {
    // Log to be concise
    setupLogging()

    val joinedDsResult = process("data/flightData.csv", "data/passengers.csv", "local[*]")
    joinedDsResult.show(100) // output to console or write to a file

    // would have cost impact due to repartition to a single node
    // writeToAFile(joinedDsResult, "data/output/noOfFlightsByMonth.csv")
  }

  def process(flightDataFilePath: String, passengerDataFilePath: String, masterURL: String) = {
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalNumberOfFlightsByMonth")
      .master(masterURL)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Load each line of the source data into an Dataset
    val dsFlights = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

    val dsPassengers = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(passengerDataFilePath)
      .as[PassengersData]
      .cache()

    val passengersAndFlights = dsFlights.select(passengerId, flightId)
    val top100FrequentFlyersDs = passengersAndFlights
      .groupBy(passengerId)
      .count()
      .filter($"count" =!= 1) // to be fair to all the passengers who have taken only 1 flight
      .limit(100)
      .cache()

    top100FrequentFlyersDs
      .join(dsPassengers, passengerId)
      .orderBy(desc("count"))
      .withColumnRenamed(passengerId, outputColumn_PassengerId)
      .withColumnRenamed("count", outputColumn_NoOfFlights)
      .withColumnRenamed(firstName, outputColumn_FirstName)
      .withColumnRenamed(lastName, outputColumn_LastName)
  }
}
