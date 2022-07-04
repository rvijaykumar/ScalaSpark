package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Find the names of the 100 most frequent flyers.
 */
object FrequentFlyers {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createSparkContext("local[*]", "FrequentFlyers")

    val frequentFlyersDs = process(fileFlightsData, filePassengersData, sparkSession)
    writeToAFile(frequentFlyersDs, "data/output/frequentFlyers.csv")

    sparkSession.stop()
  }

  def process(flightDataFilePath: String, passengerDataFilePath: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val dsFlights = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

    val dsPassengers = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(passengerDataFilePath)
      .as[PassengersData]

    val passengersAndFlights = dsFlights.select(passengerId, flightId)
    val top100FrequentFlyersDs = passengersAndFlights
      .groupBy(passengerId)
      .count()
      .orderBy(desc("count"))
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
