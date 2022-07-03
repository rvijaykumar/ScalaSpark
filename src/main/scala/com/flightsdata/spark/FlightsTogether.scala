package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.sql.Date

/**
 * Find the passengers who have been on more than 3 flights together.
 */
object FlightsTogether {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createOrGetSparkContext("local[*]", "FlightsTogether")

    val greatestNoOfCountries = process(fileFlightsData, sparkSession)
//    greatestNoOfCountries.show()
  }

  def process(flightDataFilePath: String,  sparkSession: SparkSession) = {
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    // Load each line of the source data into an Dataset
    val ds = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

//    val countriesVisitedByPassenger = ds
//        .groupBy( flight)
//        .count()
//      .orderBy("count")
//
//    countriesVisitedByPassenger.show()

//
//    def flownTogether(atLeastNTimes: Int) = {...
//    }
//
//    def flownTogether(atLeastNTimes: Int, from: Date, to: Date) = {
//      ...
//    }



  }
}
