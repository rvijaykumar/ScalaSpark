package com.flightsdata.spark

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object Utilities {
  val passengerId: String = "passengerId"
  val flightId: String = "flightId"
  val from: String = "from"
  val to: String = "to"
  val flightDate: String = "date"
  val flightMonth: String = "Month"
  val firstName: String = "firstName"
  val lastName: String = "lastName"

  val dateFormat = "yyyy-MM-dd"

  val outputColumn_PassengerId = "Passenger ID"
  val outputColumn_Passenger1Id = "Passenger 1 ID"
  val outputColumn_Passenger2Id = "Passenger 2 ID"
  val outputColumn_NoOfFlights = "Number of Flights"
  val outputColumn_NoOfFlightsTogether = "Number of flights together"
  val outputColumn_FirstName = "First name"
  val outputColumn_LastName = "Last name"
  val outputColumn_LongestRun = "Longest Run"

  val fileFlightsData ="data/flightData.csv"
  val filePassengersData = "data/passengers.csv"

  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  final case class FlightsData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
  final case class PassengersData(passengerId: Int, firstName: String, lastName: String)

  // For Prod Grade application, follow the best practice as described in
  // https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties
  def createSparkContext(masterURL: String, appName: String) = {
    val sparkSession = SparkSession
      .builder
      .appName(appName)
      .master(masterURL)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  def writeToAFile(ds: sql.DataFrame, filePath: String) = {
    ds.write.mode(SaveMode.Overwrite).option("header",true).csv(filePath)
  }

}
