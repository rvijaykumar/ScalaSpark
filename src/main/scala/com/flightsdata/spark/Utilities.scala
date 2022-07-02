package com.flightsdata.spark

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SaveMode}

object Utilities {
  // Constants
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
  val outputColumn_NoOfFlights = "Number of Flights"
  val outputColumn_FirstName = "First name"
  val outputColumn_LastName = "Last name"
  val outputColumn_LongestRun = "Longest Run"

  // Logger init to Error
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  // template
  final case class FlightsData(passengerId: Int, flightId: Int, from: String, to: String, date: String)
  final case class PassengersData(passengerId: Int, firstName: String, lastName: String)

  // Write to a file
  def writeToAFile(ds: sql.DataFrame, filePath: String) = {
    ds.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(filePath)
  }

}
