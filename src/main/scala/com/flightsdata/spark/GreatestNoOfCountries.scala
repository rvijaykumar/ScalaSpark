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

    val sparkSession = createOrGetSparkContext("local[*]", "GreatestNoOfCountries")

    val greatestNoOfCountries = process(fileFlightsData, sparkSession)
    greatestNoOfCountries.show()

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
       .withColumnRenamed("passengerId", outputColumn_PassengerId)
       .withColumnRenamed("count", "Longest Run")

  }
}
