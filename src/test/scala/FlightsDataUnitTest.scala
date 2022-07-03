import com.flightsdata.spark.{FlightsTogether, FrequentFlyers, GreatestNoOfCountries, HelloWorld, TotalNumberOfFlightsByMonth}
import org.scalatest.funsuite.AnyFunSuite
import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.functions.col

import java.sql.Date

/**
 * Unit Tests to validate the processing logic of Flights Data
 * Note: Using the same test data for ease of use
 */

class FlightsDataUnitTest extends AnyFunSuite {
  test("Hello World Lines Count") {
    val result = HelloWorld.process
    assert(result === 100000)
  }

  test("Find the total number of flights for each month") {
    val sparkSession = createOrGetSparkContext("local[*]", "TotalNumberOfFlightsByMonthUnitTest")

    val result = TotalNumberOfFlightsByMonth.process(fileFlightsData, sparkSession)
    assert(result.count !== 0)
    val firstMonthTotalNoOfFlights = result.select(outputColumn_NoOfFlights).limit(1).collectAsList().get(0)(0)
    assert(firstMonthTotalNoOfFlights === 97)

    sparkSession.stop()
  }

  test("Find the names of the 100 most frequent flyers") {
    val sparkSession = createOrGetSparkContext("local[*]", "FrequentFlyersUnitTest")

    val result = FrequentFlyers.process(fileFlightsData, filePassengersData, sparkSession)
    val topPassengerId  = result.select(outputColumn_PassengerId).limit(1).collectAsList().get(0)(0)
    assert(topPassengerId === 2068)

    sparkSession.stop()
  }

  test("Find the greatest number of countries a passenger has been in without being in the UK.") {
    val sparkSession = createOrGetSparkContext("local[*]", "FrequentFlyersUnitTest")

    val result = GreatestNoOfCountries.process(fileFlightsData, sparkSession)
    val passenger148Count  = result.where(col(outputColumn_PassengerId) === "148").select(outputColumn_LongestRun).collectAsList().get(0)(0)
    assert(passenger148Count === 10)

    sparkSession.stop()
  }

  test("Find the passengers who have been on more than 3 flights together.") {
    val sparkSession = createOrGetSparkContext("local[*]", "FrequentFlyersUnitTest")

    val result = FlightsTogether.flownTogether(FlightsTogether.process(fileFlightsData, sparkSession))
    val passenger714And770Together  = result
      .where(col(outputColumn_Passenger1Id) === "714" && col(outputColumn_Passenger2Id) === "770")
      .select(outputColumn_NoOfFlightsTogether)
      .collectAsList().get(0)(0)
    assert(passenger714And770Together === 5)

    sparkSession.stop()
  }

  test("Find the passengers who have been on more than N flights together within the range (from,to).") {
    val sparkSession = createOrGetSparkContext("local[*]", "FrequentFlyersUnitTest")

    val result = FlightsTogether.flownTogether(FlightsTogether.process(fileFlightsData, sparkSession), 5, Date.valueOf("2017-05-01"), Date.valueOf("2017-12-01"))
    val passenger157And7780Together  = result
      .where(col(outputColumn_Passenger1Id) === "157" && col(outputColumn_Passenger2Id) === "7780")
      .select(outputColumn_NoOfFlightsTogether, "From", "To")
      .collectAsList().get(0)
    assert(passenger157And7780Together(0) === 6)
    assert(passenger157And7780Together(1) === "2017-09-16")
    assert(passenger157And7780Together(2) === "2017-11-11")

    sparkSession.stop()
  }
}
