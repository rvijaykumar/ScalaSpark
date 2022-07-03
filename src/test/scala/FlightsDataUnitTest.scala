import com.flightsdata.spark.{FrequentFlyers, HelloWorld, TotalNumberOfFlightsByMonth}
import org.scalatest.funsuite.AnyFunSuite
import com.flightsdata.spark.Utilities._

/**
 * Unit Tests for validate the processing logic
 * Note: Using the same test data for ease of use
 */

class FlightsDataUnitTest extends AnyFunSuite {
  test("Hello World Lines Count") {
    val result = HelloWorld.process
    assert(result === 100000)
  }


  test("Total Number Of Flights By Month") {
    val sparkSession = createOrGetSparkContext("local[*]", "TotalNumberOfFlightsByMonthUnitTest")

    val result = TotalNumberOfFlightsByMonth.process(fileFlightsData, sparkSession)
    assert(result.count !== 0)
    val output = result.select(outputColumn_NoOfFlights).limit(1).collectAsList().get(0)(0)
    assert(output === 97)

    sparkSession.stop()
  }

  test("Frequent Flyers") {
    val sparkSession = createOrGetSparkContext("local[*]", "FrequentFlyersUnitTest")

    val result = FrequentFlyers.process(fileFlightsData, filePassengersData, sparkSession)
    val topPassengerId  = result.select(outputColumn_PassengerId).limit(1).collectAsList().get(0)(0)
    assert(topPassengerId === 2068)

    sparkSession.stop()
  }


}
