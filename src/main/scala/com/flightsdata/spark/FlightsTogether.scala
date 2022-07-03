package com.flightsdata.spark

import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object GreatestNoOfCountries {

  def main(args: Array[String]): Unit = {
    // Log to be concise
    setupLogging()

    val greatestNoOfCountries = process("data/flightData.csv", "local[*]")
//    greatestNoOfCountries.show()
  }

  def process(flightDataFilePath: String,  masterURL: String) = {
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("GreatestNoOfCountries")
      .master(masterURL)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // Load each line of the source data into an Dataset
    val ds = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(flightDataFilePath)
      .as[FlightsData]

    val countriesVisitedByPassenger = ds
        .select(passengerId, from, to)
        .filter(col(from) =!= "uk" && col(to) =!= "uk")
        .groupBy(passengerId)
        .count()
      .withColumnRenamed("count", "Longest Run")
    countriesVisitedByPassenger.show()




  }
}
