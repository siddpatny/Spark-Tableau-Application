package Calulators

import datasetStructure.{FlightDatasetSchemaCleaned, NycTaxiDatasetSchemaCleaned}
import helper.{Constants, Utils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object ComparisionCalculator {
  /** Groups and Joins Taxi and Airport data to find correlation between taxi count and number of flights, and
   * taxi count and number of passengers
   */
  def getAirportPickUpTaxiCorrMonth(sc: SparkContext, spark:SparkSession) = {
    val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_pickup_date_airport/")
    taxiGroupByAirports.createOrReplaceTempView("taxi_pu")

    val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_dest_date/")
    airportsGroupedByAirports.createOrReplaceTempView("airport_dest")

    val joinedAirportTaxi = spark.sql(
      """select tp.%s, cast(MONTH(tp.%s) as int) as MONTH, tp.%s as taxi_zone, cast(tp.%s as int), cast(tp.%s as int), cast(ad.count as int) from taxi_pu tp
     JOIN airport_dest ad ON
     tp.%s = ad.%s
     AND tp.%s = ad.%s""".format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        FlightDatasetSchemaCleaned.flight_date,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        FlightDatasetSchemaCleaned.taxi_zone_dest
      ))
    joinedAirportTaxi.show()
    Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/comparision/grouped_datasets/joinedAirportTaxi_PU/", "true")

    joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")

    val corrMatrix = findCorr(sc, spark)

    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/comparision/pu_flight_count", "true")
    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/comparision/pu_flight_passenger", "true")

  }

  /** Groups and Joins Taxi and Airport data to find correlation between taxi count and number of flights, and
   * taxi count and number of passengers
   */
  def getAirportDropOffTaxiCorrMonth(sc: SparkContext, spark:SparkSession) = {
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
    Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/comparision/grouped_datasets/joinedAirportTaxi_DO/", "true")

    joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")

    val corrMatrix = findCorr(sc, spark)
    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/comparision/do_flight_count", "true")
    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/comparision/do_flight_passenger", "true")
  }

  def findCorr(sc: SparkContext, spark:SparkSession) = {
    //    spark.sql("""select distinct(YEAR) from joinedAirportTaxi""")
    var tableTaxiCount = new ListBuffer[List[Double]]()
    var tablePassengerCount = new ListBuffer[List[Double]]()
    for(m <- Constants.monthList){
      var taxiRow = new ListBuffer[Double]()
      var passengerRow = new ListBuffer[Double]()
      for(a <- Constants.taxiZoneList){
        val temp = spark.sql("""select * from joinedAirportTaxi where MONTH=%d AND %s = %s""".format(
          m,
          "taxi_zone",
          a
        ))
        val taxiCorr = Utils.getCorrelation(temp, NycTaxiDatasetSchemaCleaned.taxi_count_airport._1, "count")
        if(taxiCorr.isNaN){
          taxiRow += 0.0
        } else{
          taxiRow += taxiCorr
        }

        val passengerCorr = Utils.getCorrelation(temp, NycTaxiDatasetSchemaCleaned.passenger_count_airport._1, "count")
        if(passengerCorr.isNaN){
          passengerRow += 0.0
        } else{
          passengerRow += passengerCorr
        }
      }
      tableTaxiCount += taxiRow.toList
      tablePassengerCount += passengerRow.toList
    }
    val corrMatrixTaxiCount = Utils.convert2DListToDf(sc, spark, tableTaxiCount.toList, FlightDatasetSchemaCleaned.corrMatrixSchema)
    val corrMatrixPassengerCount = Utils.convert2DListToDf(sc, spark, tableTaxiCount.toList, FlightDatasetSchemaCleaned.corrMatrixSchema)
    corrMatrixTaxiCount.show()
    corrMatrixPassengerCount.show()
    (corrMatrixTaxiCount, corrMatrixPassengerCount)
  }

  def getTopEventsAndBusinessesWithTaxi(spark:SparkSession) = {
    val events_df = Utils.readDataframe(spark, "datasets/analytics/event_address_join/")
    events_df.createOrReplaceTempView("events_df")
    val grouped_events_df = spark.sql("""select map_zipcode, count(*) as event_count from events_df where YEAR(start_date) = 2018 group by map_zipcode order by count(*) desc""".stripMargin)

    grouped_events_df.createOrReplaceTempView("grouped_events_df")

    val taxi_df = spark.read.format("csv").option("header", "true").load("datasets/analytics/yellow_taxi_join")
    taxi_df.createOrReplaceTempView("taxi_df")

    val grouped_taxi_df = spark.sql("""select PUZipCode, count(*) as taxi_count from taxi_df where YEAR(tpep_pickup_datetime) = 2018 GROUP BY PUZipCode """).persist(StorageLevel.MEMORY_AND_DISK)

    grouped_taxi_df.createOrReplaceTempView("grouped_taxi_df")

    val event_taxi_joined = spark.sql("""select gtf.PUZipCode as Zipcode, gdf.event_count, gtf.taxi_count from grouped_events_df gdf JOIN grouped_taxi_df gtf ON gdf.map_zipcode = gtf.PUZipCode """)

    Utils.saveDataframe(event_taxi_joined,"datasets/analytics/comparision/zip_joined_2018/event_taxi/", "true")
    event_taxi_joined.show()

    val business_df = spark.read.format("csv").option("header", "true").load("datasets/cleanedEBDataset/csv_format/business")
    business_df.createOrReplaceTempView("business_df")

    val grouped_business_df = spark.sql("""select zipcode, count(*) as business_count from business_df where YEAR(start_date) <= 2018 AND YEAR(end_date) >= 2018 AND status = "Active" group by zipcode Order By count(*) desc""")
    grouped_business_df.createOrReplaceTempView("grouped_business_df")

    val business_taxi_joined = spark.sql("""select gtf.PUZipCode as Zipcode, gbf.business_count, gtf.taxi_count from grouped_business_df gbf JOIN grouped_taxi_df gtf ON gbf.zipcode = gtf.PUZipCode """)

    Utils.saveDataframe(business_taxi_joined,"datasets/analytics/comparision/zip_joined_2018/business_taxi/", "true")
    business_taxi_joined.show()
//    val airport_df = spark.read.format("csv").option("header", "true").load("datasets/cleanedFlightDataset/nyc_airports/")
//    airport_df.createOrReplaceTempView("airport_df")

  }

  //  def getAirportDropOffTaxiCorrDiff(sc: SparkContext, spark:SparkSession) = {
  //    val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_drop_date_airport/")
  //    taxiGroupByAirports.createOrReplaceTempView("taxi_pu")
  //
  //    val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_origin_date/")
  //    airportsGroupedByAirports.createOrReplaceTempView("airport_origin")
  //
  //    val joinedAirportTaxi = spark.sql(
  //      """select tp.%s, cast(MONTH(tp.%s) as int) as MONTH, tp.%s as taxi_zone, cast(tp.%s as int), cast(tp.%s as int), cast(ao.count as int) from taxi_pu tp
  //     JOIN airport_origin ao ON
  //     tp.%s = ao.%s
  //     AND tp.%s = ao.%s""".format(
  //        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
  //        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //        FlightDatasetSchemaCleaned.flight_date,
  //        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
  //        FlightDatasetSchemaCleaned.taxi_zone_orig
  //      ))
  //    joinedAirportTaxi.show()
  //    Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/correlation/grouped_datasets/joinedAirportTaxi_DO/", "true")
  //
  //    joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")
  //
  //    val corrMatrix = findCorr(sc, spark)
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_count", "true")
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_passenger", "true")
  //  }
  //  def getAirportDropOffTaxiCorr(sc: SparkContext, spark:SparkSession) = {
  //    val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_drop_date_airport/")
  //    taxiGroupByAirports.createOrReplaceTempView("taxi_pu")
  //    val yearGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), %s""".format(
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.DOLocationID._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.DOLocationID._1))
  //
  //    val monthGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR , MONTH(%s) as MONTH, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), MONTH(%s), %s""".format(
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.DOLocationID._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.DOLocationID._1))
  //
  //    monthGroupedTaxi.createOrReplaceTempView("monthGroupedTaxi")
  //
  //    val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_origin_date/")
  //    airportsGroupedByAirports.createOrReplaceTempView("airport_origin")
  //
  //    val yearGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, %s, sum(count) as count from airport_origin GROUP BY %s, YEAR(%s)""".format(
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.taxi_zone_orig,
  //      FlightDatasetSchemaCleaned.taxi_zone_orig,
  //      FlightDatasetSchemaCleaned.flight_date))
  //
  //    val monthGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, MONTH(%s) as MONTH, %s, sum(count) as count from airport_origin GROUP BY %s, YEAR(%s), MONTH(%s)""".format(
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.taxi_zone_orig,
  //      FlightDatasetSchemaCleaned.taxi_zone_orig,
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.flight_date))
  //
  //    monthGroupedAirport.createOrReplaceTempView("monthGroupedAirport")
  //
  //    val joinedAirportTaxi = spark.sql(
  //      """select mt.YEAR, mt.MONTH, count, %s as taxi_zone, %s, %s from monthGroupedAirport ma
  //     JOIN monthGroupedTaxi mt ON
  //     ma.YEAR = mt.YEAR
  //     AND ma.MONTH = mt.MONTH
  //     AND ma.%s = mt.%s""".format(
  //        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
  //        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //        FlightDatasetSchemaCleaned.taxi_zone_orig,
  //        NycTaxiDatasetSchemaCleaned.DOLocationID._1
  //      ))
  //
  //    Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/grouped_datasets/joinedAirportTaxi_DO/", "true")
  //
  //    joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")
  //
  //    val corrMatrix = findCorr(sc, spark)
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_count", "true")
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/do_flight_passenger", "true")
  //  }

  //  def getAirportPickUpTaxiCorr(sc: SparkContext, spark:SparkSession) = {
  //    val taxiGroupByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/taxi_pickup_date_airport/")
  //    taxiGroupByAirports.createOrReplaceTempView("taxi_pu")
  //    val yearGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), %s""".format(
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.PULocationID._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.PULocationID._1))
  //
  //    val monthGroupedTaxi = spark.sql("""select YEAR(%s) as YEAR , MONTH(%s) as MONTH, %s, sum(%s) as %s, sum(%s) as %s from taxi_pu GROUP BY YEAR(%s), MONTH(%s), %s""".format(
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.PULocationID._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
  //      NycTaxiDatasetSchemaCleaned.PULocationID._1))
  //
  //    monthGroupedTaxi.createOrReplaceTempView("monthGroupedTaxi")
  //
  //    val airportsGroupedByAirports = Utils.readDataframe(spark, "datasets/analytics/grouped_datasets/airport_dest_date/")
  //    airportsGroupedByAirports.createOrReplaceTempView("airport_dest")
  //
  //    val yearGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, %s, sum(count) as count from airport_dest GROUP BY %s, YEAR(%s)""".format(
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.taxi_zone_dest,
  //      FlightDatasetSchemaCleaned.taxi_zone_dest,
  //      FlightDatasetSchemaCleaned.flight_date))
  //
  //    val monthGroupedAirport = spark.sql("""select YEAR(%s) as YEAR, MONTH(%s) as MONTH, %s, sum(count) as count from airport_dest GROUP BY %s, YEAR(%s), MONTH(%s)""".format(
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.taxi_zone_dest,
  //      FlightDatasetSchemaCleaned.taxi_zone_dest,
  //      FlightDatasetSchemaCleaned.flight_date,
  //      FlightDatasetSchemaCleaned.flight_date))
  //
  //    monthGroupedAirport.createOrReplaceTempView("monthGroupedAirport")
  //
  //    val joinedAirportTaxi = spark.sql(
  //      """select mt.YEAR, mt.MONTH, count, %s as taxi_zone, %s, %s from monthGroupedAirport ma
  //     JOIN monthGroupedTaxi mt ON
  //     ma.YEAR = mt.YEAR
  //     AND ma.MONTH = mt.MONTH
  //     AND ma.%s = mt.%s""".format(
  //        NycTaxiDatasetSchemaCleaned.PULocationID._1,
  //        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
  //        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
  //        FlightDatasetSchemaCleaned.taxi_zone_dest,
  //        NycTaxiDatasetSchemaCleaned.PULocationID._1
  //      ))
  //
  //    Utils.saveDataframe(joinedAirportTaxi, "datasets/analytics/grouped_datasets/joinedAirportTaxi_PU/", "true")
  //
  //    joinedAirportTaxi.createOrReplaceTempView("joinedAirportTaxi")
  //
  //    val corrMatrix = findCorr(sc, spark)
  //
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/pu_flight_count", "true")
  //    Utils.saveDataframe(corrMatrix._1, "datasets/analytics/correlation/pu_flight_passenger", "true")
  //
  //  }
}
