package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

/**
 * Find the greatest number of countries a passenger has been in without being in the UK.
 */
object GreatestNoOfCountries {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createSparkContext("local[*]", "GreatestNoOfCountries")

    val greatestNoOfCountriesDs = process(fileFlightsData, sparkSession)
    writeToAFile(greatestNoOfCountriesDs, "data/output/greatestNoOfCountries.csv")

    sparkSession.stop()
  }

  def process(flightDataFilePath: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val ds = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

     ds.select(passengerId, from, to)
        .filter(col(from) =!= "uk" && col(to) =!= "uk")
        .groupBy(passengerId)
        .count()
        .orderBy("count")
        .withColumnRenamed("passengerId", outputColumn_PassengerId)
        .withColumnRenamed("count", "Longest Run")

  }
}
