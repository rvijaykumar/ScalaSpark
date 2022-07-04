package com.flightsdata.spark

import org.apache.spark._
import org.apache.log4j._

/**
 * Hello World Lines Count
 * To make sure the project setup is working with Scala/Spark libs
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    println(
      args(0)
    )
    val numLines = process
    println("Hello world! The file has " + numLines + " lines.")
  }

   def process = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloWorld")

    val lines = sc.textFile("data/u.data")
    lines.count()
  }
}