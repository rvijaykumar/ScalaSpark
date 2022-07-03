import com.flightsdata.spark.{FlightsTogether, FrequentFlyers, GreatestNoOfCountries, HelloWorld, TotalNumberOfFlightsByMonth}
import org.scalatest.funsuite.AnyFunSuite
import com.flightsdata.spark.Utilities._
import org.apache.spark.sql.functions.col

/**
 * Unit Tests for validate the processing logic
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

    val result = FlightsTogether.process(fileFlightsData, sparkSession)
    val passenger714And770Together  = result
      .where(col(outputColumn_Passenger1Id) === "714" && col(outputColumn_Passenger2Id) === "770")
      .select(outputColumn_NoOfFlightsTogether)
      .collectAsList().get(0)(0)
    assert(passenger714And770Together === 5)

    sparkSession.stop()
  }
}
