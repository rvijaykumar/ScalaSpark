package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, month, to_date}

/**
 * Find the total number of flights for each month.
 */
object TotalNumberOfFlightsByMonth {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createOrGetSparkContext("local[*]", "TotalNumberOfFlightsByMonth")

    val totalNoOfFlightsByMonth = process(fileFlightsData, sparkSession)
    writeToAFile(totalNoOfFlightsByMonth, "data/output/noOfFlightsByMonth.csv")

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

    val flightsByMonth = ds.withColumn(flightMonth, month(to_date(col(flightDate), dateFormat)))
    val distinctFlightsByMonth = flightsByMonth.select(flightMonth, flightId, from, to).distinct()

     distinctFlightsByMonth
      .groupBy(flightMonth)
      .count()
      .orderBy(flightMonth)
      .withColumnRenamed("count", outputColumn_NoOfFlights)
  }
}
