package Calulators

import datasetStructure.{FlightDatasetSchemaCleaned, NycTaxiDatasetSchemaCleaned}
import helper.{Constants, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object ComparisionCalculator {

    def getAirportDropOffTaxiCorrDiff(sc: SparkContext, spark:SparkSession) = {
      val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_drop_date_airport/")
      taxiGroupByAirports.createOrReplaceTempView("taxi_pu")

      val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_origin_date/")
      airportsGroupedByAirports.createOrReplaceTempView("airport_origin")

      val joinedAirportTaxi = spark.sql(
        """select tp.%s, cast(MONTH(tp.%s) as int) as MONTH, tp.%s as taxi_zone, cast(tp.%s as int), cast(tp.%s as int), cast(ao.count as int) from taxi_pu tp
       JOIN airport_origin ao ON
       tp.%s = ao.%s
       AND tp.%s = ao.%s""".format(
          NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
          NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
          NycTaxiDatasetSchemaCleaned.DOLocationID._1,
          NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
          NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
          NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
          FlightDatasetSchemaCleaned.flight_date,
          NycTaxiDatasetSchemaCleaned.DOLocationID._1,
          FlightDatasetSchemaCleaned.taxi_zone_orig
        ))
      joinedAirportTaxi.show()
      Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/correlation/grouped_datasets/joinedAirportTaxi_DO/", "true")

      joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")

      val corrMatrix = findCorr(sc, spark)
      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_count", "true")
      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_passenger", "true")
    }
    def getAirportDropOffTaxiCorr(sc: SparkContext, spark:SparkSession) = {
      val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_drop_date_airport/")
      taxiGroupByAirports.createOrReplaceTempView("taxi_pu")
      val yearGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), %s""".format(
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1))

      val monthGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR , MONTH(%s) as MONTH, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), MONTH(%s), %s""".format(
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1))

      monthGroupedTaxi.createOrReplaceTempView("monthGroupedTaxi")

      val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_origin_date/")
      airportsGroupedByAirports.createOrReplaceTempView("airport_origin")

      val yearGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, %s, sum(count) as count from airport_origin GROUP BY %s, YEAR(%s)""".format(
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.taxi_zone_orig,
        FlightDatasetSchemaCleaned.taxi_zone_orig,
        FlightDatasetSchemaCleaned.flight_date))

      val monthGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, MONTH(%s) as MONTH, %s, sum(count) as count from airport_origin GROUP BY %s, YEAR(%s), MONTH(%s)""".format(
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.taxi_zone_orig,
        FlightDatasetSchemaCleaned.taxi_zone_orig,
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.flight_date))

      monthGroupedAirport.createOrReplaceTempView("monthGroupedAirport")

      val joinedAirportTaxi = spark.sql(
        """select mt.YEAR, mt.MONTH, count, %s as taxi_zone, %s, %s from monthGroupedAirport ma
       JOIN monthGroupedTaxi mt ON
       ma.YEAR = mt.YEAR
       AND ma.MONTH = mt.MONTH
       AND ma.%s = mt.%s""".format(
          NycTaxiDatasetSchemaCleaned.DOLocationID._1,
          NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
          NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
          FlightDatasetSchemaCleaned.taxi_zone_orig,
          NycTaxiDatasetSchemaCleaned.DOLocationID._1
        ))

      Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/grouped_datasets/joinedAirportTaxi_DO/", "true")

      joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")

      val corrMatrix = findCorr(sc, spark)
      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_count", "true")
      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_passenger", "true")
    }

    def getAirportPickUpTaxiCorr(sc: SparkContext, spark:SparkSession) = {
      val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_pickup_date_airport/")
      taxiGroupByAirports.createOrReplaceTempView("taxi_pu")
      val yearGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), %s""".format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1))

      val monthGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR , MONTH(%s) as MONTH, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), MONTH(%s), %s""".format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1))

      monthGroupedTaxi.createOrReplaceTempView("monthGroupedTaxi")

      val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_dest_date/")
      airportsGroupedByAirports.createOrReplaceTempView("airport_dest")

      val yearGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, %s, sum(count) as count from airport_dest GROUP BY %s, YEAR(%s)""".format(
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.taxi_zone_dest,
        FlightDatasetSchemaCleaned.taxi_zone_dest,
        FlightDatasetSchemaCleaned.flight_date))

      val monthGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, MONTH(%s) as MONTH, %s, sum(count) as count from airport_dest GROUP BY %s, YEAR(%s), MONTH(%s)""".format(
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.taxi_zone_dest,
        FlightDatasetSchemaCleaned.taxi_zone_dest,
        FlightDatasetSchemaCleaned.flight_date,
        FlightDatasetSchemaCleaned.flight_date))

      monthGroupedAirport.createOrReplaceTempView("monthGroupedAirport")

      val joinedAirportTaxi = spark.sql(
        """select mt.YEAR, mt.MONTH, count, %s as taxi_zone, %s, %s from monthGroupedAirport ma
       JOIN monthGroupedTaxi mt ON
       ma.YEAR = mt.YEAR
       AND ma.MONTH = mt.MONTH
       AND ma.%s = mt.%s""".format(
          NycTaxiDatasetSchemaCleaned.PULocationID._1,
          NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
          NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
          FlightDatasetSchemaCleaned.taxi_zone_dest,
          NycTaxiDatasetSchemaCleaned.PULocationID._1
        ))

      Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/grouped_datasets/joinedAirportTaxi_PU/", "true")

      joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")

      val corrMatrix = findCorr(sc, spark)

      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/pu_flight_count", "true")
      Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/pu_flight_passenger", "true")

    }
}
