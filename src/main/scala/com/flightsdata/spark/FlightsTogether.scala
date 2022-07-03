package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, min, to_date}

import java.sql.Date

/**
 * Find the passengers who have been on more than 3 flights together.
 */
object FlightsTogether {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createOrGetSparkContext("local[*]", "FlightsTogether")

    val flightsTogetherDs = process(fileFlightsData, sparkSession)

    // Passengers who have been on more than 3 flights together.
    val threeFlightsTogetherDs = flownTogether(flightsTogetherDs)
    writeToAFile(threeFlightsTogetherDs, "data/output/moreThan3FlightsTogether.csv")

    // Passengers who have been on more than N flights together within the range (from,to).
    val flightsTogetherByDatesDs = flownTogether(flightsTogetherDs, 5, Date.valueOf("2017-05-01"), Date.valueOf("2017-12-01"))
    writeToAFile(flightsTogetherByDatesDs, "data/output/moreThanNFlightsTogetherBetweenDates.csv")
  }

  def process(flightDataFilePath: String,  sparkSession: SparkSession) = {
    import sparkSession.implicits._

    // Load each line of the source data into an Dataset
    val dsFlights = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]
      .select(passengerId, flightId, flightDate)

    // As the output requires passenger Ids from the same dataset in 2 columns and mapping between the two,
    // we need to self-join on the same dataset.
    // Use < to compare the 2 DataSets on Passenger Ids to remove duplicates
    // Using =!= gives some duplicates as the same passendger will get repeated in Passenger 1 Id & Passenger 2 Id columns
    dsFlights.as("dsRight")
      .withColumnRenamed(passengerId, outputColumn_Passenger1Id)
      .join(dsFlights.as("dsLeft"))
      .withColumnRenamed(passengerId, outputColumn_Passenger2Id)
      .where(col(outputColumn_Passenger1Id) < col(outputColumn_Passenger2Id) &&
        $"dsRight.flightId" === $"dsLeft.flightId" &&
        $"dsRight.date" === $"dsLeft.date")
      .cache()
  }

  /**
   * Find the passengers who have been on more than 3 flights
   * @param flightsTogetherDs
   * @return
   */
  def flownTogether(flightsTogetherDs: sql.DataFrame) = {
    flightsTogetherDs
      .groupBy(outputColumn_Passenger1Id, outputColumn_Passenger2Id)
      .count()
      .where(col("count") > 3)
      .withColumnRenamed("count", outputColumn_NoOfFlightsTogether)
  }
  /**
   * Find the passengers who have been on more than N flights together within the range (from,to).
   * @param flightsTogetherDs
   * @param atLeastNTimes
   * @param from
   * @param to
   */
  def flownTogether(flightsTogetherDs: sql.DataFrame, atLeastNTimes: Int, from: Date, to: Date) = {
    flightsTogetherDs
      .where(to_date(col("dsRight.date"), dateFormat) between( from, to))
      .groupBy(outputColumn_Passenger1Id, outputColumn_Passenger2Id)
      .agg(count("*").as(outputColumn_NoOfFlightsTogether),
        min(col("dsRight.date")).as("From"),
        max(col("dsRight.date")).as("To"))
      .where(col(outputColumn_NoOfFlightsTogether) > atLeastNTimes)
  }
}
