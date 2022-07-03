package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * Find the passengers who have been on more than 3 flights together.
 */
object FlightsTogether {
  def main(args: Array[String]): Unit = {
    setupLogging()

    val sparkSession = createOrGetSparkContext("local[*]", "FlightsTogether")

    val flightsTogetherDs = process(fileFlightsData, sparkSession)
    writeToAFile(flightsTogetherDs, "data/output/moreThan3FlightsTogether.csv")
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
    dsFlights.as("dsRight")
      .withColumnRenamed(passengerId, outputColumn_Passenger1Id)
      .join(dsFlights.as("dsLeft"))
      .withColumnRenamed(passengerId, outputColumn_Passenger2Id)
      .where(col(outputColumn_Passenger1Id) < col(outputColumn_Passenger2Id) &&
        $"dsRight.flightId" === $"dsLeft.flightId" &&
        $"dsRight.date" === $"dsLeft.date")
      .cache()
      .groupBy(outputColumn_Passenger1Id, outputColumn_Passenger2Id)
      .count()
      .where(col("count") > 3)
      .withColumnRenamed("count", outputColumn_NoOfFlightsTogether)
  }
}
