package Calulators

import datasetStructure.{AddressMappingSchema, BusinessDatasetSchema, EventsDatasetSchema, FlightDatasetSchemaCleaned, LyftDatasetSchemaCleaned, NycTaxiDatasetSchemaCleaned, NycTaxiDatasetSchemaMapped, UberDatasetSchemaCleaned}
import helper.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, stddev}

/** Functions to calculate the Analytics
 */
object AnalyticsCalulators {

  /** Group Airports by airport locations and date
   */
  def getAirportsGroupByAirports(spark:SparkSession) = {
    val airports_df = Utils.readDataframe(spark, "datasets/cleanedFlightDataset/nyc_airports/")

    airports_df.createOrReplaceTempView("airports_df")

    val grouped_dest_airport_df = spark.sql("SELECT %s, %s, COUNT(*) as count FROM airports_df where %s is NOT NULL GROUP BY %s, %s".format(
      FlightDatasetSchemaCleaned.flight_date,
      FlightDatasetSchemaCleaned.taxi_zone_dest,
      FlightDatasetSchemaCleaned.taxi_zone_dest,
      FlightDatasetSchemaCleaned.flight_date,
      FlightDatasetSchemaCleaned.taxi_zone_dest
    ))

    Utils.saveDataframe(grouped_dest_airport_df, "datasets/analytics/grouped_datasets/airport_dest_date/", "true")

    val grouped_orig_airport_df = spark.sql("SELECT %s, %s, COUNT(*) as count FROM airports_df where %s is NOT NULL GROUP BY %s, %s".format(
      FlightDatasetSchemaCleaned.flight_date,
      FlightDatasetSchemaCleaned.taxi_zone_orig,
      FlightDatasetSchemaCleaned.taxi_zone_orig,
      FlightDatasetSchemaCleaned.flight_date,
      FlightDatasetSchemaCleaned.taxi_zone_orig
    ))

    Utils.saveDataframe(grouped_orig_airport_df, "datasets/analytics/grouped_datasets/airport_origin_date/", "true")

    (grouped_orig_airport_df, grouped_dest_airport_df)

  }

  /** Group Lyft by airport locations and date
   */
  def getLyftGroupByAirports(spark:SparkSession) = {
    val lyftDateCount = spark.sql(
      """select %s, %s, count(*) as taxi_count_airport, sum(%s) as passenger_count_airport
        FROM lyftMappedDf
        GROUP BY %s , %s
        HAVING %s IN (1, 132, 138)""".stripMargin.format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.passenger_count._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1))

    Utils.saveDataframe(lyftDateCount, "datasets/analytics/grouped_datasets/lyft_date_airport/", "true")
    lyftDateCount.show()
    lyftDateCount
  }

  /** Group Uber by airport locations and date
   */
  def getUberGroupByAirports(spark:SparkSession) = {
    val uberDateCount = spark.sql(
      """select %s, %s, count(*) as taxi_count_airport, sum(%s) as passenger_count_airport
        FROM uberMappedDf
        GROUP BY %s , %s
        HAVING %s IN (1, 132, 138)""".stripMargin.format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.passenger_count._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1))

    Utils.saveDataframe(uberDateCount, "datasets/analytics/grouped_datasets/uber_date_airport/", "true")
    uberDateCount.show()
    uberDateCount
  }

  /** Group Taxi by airport locations and date
   */
  def getTaxiGroupByAirports(spark:SparkSession) = {
    val taxiPuDateCount = spark.sql(
      """select %s, %s, count(*) as %s, sum(%s) as %s
        FROM yellowTaxiDataframe
        GROUP BY %s , %s
        HAVING %s IN (1, 132, 138)""".stripMargin.format(
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1,
        NycTaxiDatasetSchemaCleaned.pickupDateTime._1,
        NycTaxiDatasetSchemaCleaned.PULocationID._1))

    Utils.saveDataframe(taxiPuDateCount, "datasets/analytics/grouped_datasets/taxi_pickup_date_airport/", "true")
    taxiPuDateCount.show()

    val taxiDropDateCount = spark.sql(
      """select %s, %s, count(*) as %s, sum(%s) as %s
        FROM yellowTaxiDataframe
        GROUP BY %s , %s
        HAVING %s IN (1, 132, 138)""".stripMargin.format(
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
        NycTaxiDatasetSchemaCleaned.taxi_count_airport._1,
        NycTaxiDatasetSchemaCleaned.passenger_count._1,
        NycTaxiDatasetSchemaCleaned.passenger_count_airport._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1,
        NycTaxiDatasetSchemaCleaned.dropOffDateTime._1,
        NycTaxiDatasetSchemaCleaned.DOLocationID._1))

    Utils.saveDataframe(taxiDropDateCount, "datasets/analytics/grouped_datasets/taxi_drop_date_airport/", "true")
    taxiDropDateCount.show()

    (taxiPuDateCount, taxiDropDateCount)
  }

  /** Join Events with address mapping obtained from BING API
   */
  def event_address_join(spark:SparkSession):Unit = {
    val eventDF =  Utils.convertRddToDf(spark, "datasets/cleanedEBDataset/csv_format/events/", EventsDatasetSchema.customSchema)
    val addressDF =  Utils.convertRddToDf(spark, "datasets/events_address_mapping/", AddressMappingSchema.customSchema)
    eventDF.createOrReplaceTempView("event_table")
    addressDF.createOrReplaceTempView("address_table")

    val event_address_df = spark.sql("select et.*, at.zipcode as map_zipcode " +
      " FROM event_table et " +
      " LEFT JOIN address_table at " +
      " ON et.address = at.address and et.borough = at.borough")

    Utils.saveDataframeMultiple(event_address_df, "datasets/analytics/event_address_join/")
  }

  /** Get time series for all the datasets
   */
  def getTimeSeries(spark:SparkSession): Unit = {

    val event_df = spark.read
      .format("csv")
      .schema(EventsDatasetSchema.customSchema2)
      .option("header", "true")
      .load("datasets/analytics/event_address_join/")

    //    val taxi_df = spark.read
    //      .format("csv")
    //      .schema(NycTaxiDatasetSchemaMapped.customSchema)
    //      .option("header", "true")
    //      .load("datasets/analytics/yellow_taxi_join/")


    event_df.createOrReplaceTempView("event_df")
    //    taxi_df.createOrReplaceTempView("taxi_df")

    //    val grouped_df = spark.sql("""
    //    SELECT start_date, count(*) AS events
    //    FROM event_df
    //    GROUP BY start_date
    //    ORDER BY start_date ASC NULLS LAST""")

    val event_ts = spark.sql("""
      SELECT start_date, map_zipcode as zip, count(*) as amount
      FROM event_df
      GROUP BY start_date, zip
      GROUPING SETS ((start_date,zip), (zip))
      ORDER BY start_date ASC NULLS LAST
      """)

    val grouped_year_df = spark.sql("""
    SELECT YEAR(start_date) as year, count(*) AS events
    FROM event_df
    GROUP BY YEAR(start_date)
    ORDER BY YEAR(start_date) ASC NULLS LAST""")
    //    val grouped_df = taxi_df.groupBy(window(taxi_df.col("tpep_pickup_datetime"), "1 day")).count()

    val taxi_ts = spark.sql("""
      SELECT %s as start_date,%s as zip, count(*) as amount
      FROM mappedYellowTaxi
      GROUP BY start_date, zip
      GROUPING SETS ((start_date,zip), (zip))
      ORDER BY start_date ASC NULLS LAST
      """.format(NycTaxiDatasetSchemaMapped.pickupDateTime._1, NycTaxiDatasetSchemaMapped.PUZipCode._1))
    println("EVENT YEAR GROUP")
    //    grouped_year_df.show(20)

    println("TAXI TS")
    //    taxi_ts.show(20)

    println("EVENT TS")
    //    event_ts.show(20)

    event_ts.createOrReplaceTempView("event_ts")
    taxi_ts.createOrReplaceTempView("taxi_ts")

    //    val outer_joined_event_taxi = spark.sql("""
    //      SELECT t.start_date as t_start_date, e.start_date as e_start_date, t.zip as taxi_zip, e.zip as e_zip, e.amount as e_count, t.amount as t_count
    //      FROM taxi_ts as t FULL OUTER JOIN event_ts as e
    //      ON (t.start_date = e.start_date AND e.zip = t.zip)
    //      ORDER BY t.zip ASC NULLS FIRST
    //      """.stripMargin)
    //
    //    val outer_mean_stddev_Df = outer_joined_event_taxi.select(mean("e_count").alias("e_count_mean"),
    //      mean("t_count").alias("t_count_mean"),
    //      stddev("e_count").alias("e_count_stddev"),
    //      stddev("t_count").alias("t_count_stddev"))
    //
    //    val outer_e_mean  = outer_mean_stddev_Df.first().get(0)
    //    val outer_t_mean = outer_mean_stddev_Df.first().get(1)
    //    val outer_e_stddev = outer_mean_stddev_Df.first().get(2)
    //    val outer_t_stddev = outer_mean_stddev_Df.first().get(3)
    //
    //
    //    val outer_mean_stddev = outer_joined_event_taxi.withColumn("e_count_normalised", (col("e_count")-outer_e_mean)/outer_e_stddev)
    //      .withColumn("t_count_normalised", (col("t_count")-outer_t_mean)/outer_t_stddev)
    //
    //    println("JOINED EVENT AND TAXI")

    //    Utils.saveDataframeMultiple(outer_joined_event_taxi, "datasets/analytics/joined_datasets/outer_event_taxi_ts/")
    //    Utils.saveDataframeMultiple(outer_mean_stddev, "datasets/analytics/joined_datasets/outer_event_taxi_normalised_ts/")

    ///////////// JOIN /////////////////

    val join_joined_event_taxi = spark.sql("""
      SELECT t.start_date as start_date, t.zip as taxi_zip, e.zip as e_zip, e.amount as e_count, t.amount as t_count
      FROM taxi_ts as t JOIN event_ts as e
      ON (t.start_date = e.start_date AND e.zip = t.zip)
      ORDER BY t.zip ASC NULLS FIRST
      """.stripMargin)

    val join_mean_stddev_Df = join_joined_event_taxi.select(mean("e_count").alias("e_count_mean"),
      mean("t_count").alias("t_count_mean"),
      stddev("e_count").alias("e_count_stddev"),
      stddev("t_count").alias("t_count_stddev"))

    val join_e_mean  = join_mean_stddev_Df.first().get(0)
    val join_t_mean = join_mean_stddev_Df.first().get(1)
    val join_e_stddev = join_mean_stddev_Df.first().get(2)
    val join_t_stddev = join_mean_stddev_Df.first().get(3)


    val join_mean_stddev = join_joined_event_taxi.withColumn("e_count_normalised", (col("e_count")-join_e_mean)/join_e_stddev)
      .withColumn("t_count_normalised", (col("t_count")-join_t_mean)/join_t_stddev)

    Utils.saveDataframeMultiple(join_joined_event_taxi, "datasets/analytics/joined_datasets/join_event_taxi_ts/")
    Utils.saveDataframeMultiple(join_mean_stddev, "datasets/analytics/joined_datasets/join_event_taxi_normalised_ts/")


  }

  /** Join Businesses with Taxi
   */
  def joinBusinessTaxi(spark:SparkSession) = {

    val business_df = spark.read
      .format("csv")
      .schema(EventsDatasetSchema.customSchema2)
      .option("header", "true")
      .load("datasets/cleanedEBDataset/csv_format/business/*.csv")

    business_df.createOrReplaceTempView("business_df")

    val business_ts = spark.sql("""
      SELECT start_date, zipcode as zip, count(*) as amount
      FROM business_df
      GROUP BY %s, %s
      GROUPING SETS ((%s, %s), (%s))
      ORDER BY %s ASC NULLS LAST
      """.format(BusinessDatasetSchema.start_date, BusinessDatasetSchema.zipcode,
      BusinessDatasetSchema.start_date, BusinessDatasetSchema.zipcode, BusinessDatasetSchema.zipcode,
      BusinessDatasetSchema.start_date))

    business_ts.createOrReplaceTempView("business_ts")

    val join_joined_business_taxi = spark.sql("""
      SELECT t.start_date as start_date, t.zip as taxi_zip, b.zip as b_zip, b.amount as b_count, t.amount as t_count
      FROM taxi_ts as t JOIN business_ts as b
      ON (t.start_date = b.start_date AND b.zip = t.zip)
      ORDER BY t.zip ASC NULLS FIRST
      """.stripMargin)

    val join_mean_stddev_Df = join_joined_business_taxi.select(mean("b_count").alias("b_count_mean"),
      mean("t_count").alias("t_count_mean"),
      stddev("b_count").alias("b_count_stddev"),
      stddev("t_count").alias("t_count_stddev"))

    val join_e_mean  = join_mean_stddev_Df.first().get(0)
    val join_t_mean = join_mean_stddev_Df.first().get(1)
    val join_e_stddev = join_mean_stddev_Df.first().get(2)
    val join_t_stddev = join_mean_stddev_Df.first().get(3)


    val join_mean_stddev = join_joined_business_taxi.withColumn("b_count_normalised", (col("b_count")-join_e_mean)/join_e_stddev)
      .withColumn("t_count_normalised", (col("t_count")-join_t_mean)/join_t_stddev)

    Utils.saveDataframeMultiple(join_joined_business_taxi, "datasets/analytics/joined_datasets/join_business_taxi_ts/")
    Utils.saveDataframeMultiple(join_mean_stddev, "datasets/analytics/joined_datasets/join_business_taxi_normalised_ts/")

  }

  /** Group the datasets by Year and Zip
   */
  def getYearAndZipCounts(spark:SparkSession) = {

    /////// EVENTS /////////
    val event_df = spark.read
      .format("csv")
      .schema(EventsDatasetSchema.customSchema2)
      .option("header", "true")
      .load("datasets/analytics/event_address_join/")

    event_df.createOrReplaceTempView("event_table")

    val eventZipCount = spark.sql("Select count(*) as event_count, map_zipcode as zip " +
      "FROM event_table " +
      "GROUP BY map_zipcode ")

    Utils.saveDataframe(eventZipCount, "datasets/analytics/exploration/events_zip/", "true")

    val eventYearCount = spark.sql("""
      SELECT YEAR(start_date) as year, count(*) AS event_count
      FROM event_table
      GROUP BY YEAR(start_date)
      ORDER BY YEAR(start_date) ASC NULLS LAST""")

    Utils.saveDataframe(eventYearCount, "datasets/analytics/exploration/events_year/", "true")

    /////// BUSINESS ///////
    val businessDf = Utils.convertRddToDf(spark, "datasets/cleanedEBDataset/csv_format/business/", BusinessDatasetSchema.customSchema)

    businessDf.createOrReplaceTempView("business_table")

    val businessCountDf = spark.sql("Select count(*) as business_count, zipcode as zip " +
      "FROM business_table " +
      "GROUP BY zipcode " +
      "ORDER BY zipcode ASC NULLS LAST"
    )

    val businessYearCount = spark.sql("""
      SELECT YEAR(end_date) as year, count(*) AS business_count
      FROM business_table
      GROUP BY YEAR(end_date)
      ORDER BY YEAR(end_date) ASC NULLS LAST""")

    Utils.saveDataframe(businessCountDf, "datasets/analytics/exploration/business_zip/", "true")
    Utils.saveDataframe(businessYearCount, "datasets/analytics/exploration/business_year/", "true")

    /////// UBER //////////

    val uber_df = spark.read
      .format("csv")
      .schema(UberDatasetSchemaCleaned.customSchema2)
      .option("header", "true")
      .load("datasets/analytics/uber_join/")

    uber_df.createOrReplaceTempView("uber_table")

    val uberZipCount = spark.sql("Select count(*) as uber_count, zipcode as zip " +
      "FROM uber_table " +
      "GROUP BY zipcode " +
      "ORDER BY zipcode ASC NULLS LAST"
    )

    Utils.saveDataframe(uberZipCount, "datasets/analytics/exploration/uber_zip/", "true")
    uberZipCount.show()
    val uberYearCount = spark.sql("""
      SELECT YEAR(%s) as year, count(*) AS uber_count
      FROM uber_table
      GROUP BY YEAR(%s)
      ORDER BY YEAR(%s) ASC NULLS LAST""".format(UberDatasetSchemaCleaned.dateTime._1, UberDatasetSchemaCleaned.dateTime._1
      , UberDatasetSchemaCleaned.dateTime._1))

    Utils.saveDataframe(uberYearCount, "datasets/analytics/exploration/uber_year/", "true")
    uberYearCount.show()
    /////// YELLOW TAXI //////////

    val taxi_df = spark.read
      .format("csv")
      .schema(NycTaxiDatasetSchemaMapped.customSchema)
      .option("header", "true")
      .load("datasets/analytics/yellow_taxi_join/")

    taxi_df.createOrReplaceTempView("taxi_table")

    val taxiZipCount = spark.sql("Select count(*) as taxi_count, PUZipCode as zip " +
      "FROM taxi_table " +
      "GROUP BY PUZipCode " +
      "ORDER BY PUZipCode ASC NULLS LAST"
    )

    Utils.saveDataframe(taxiZipCount, "datasets/analytics/exploration/taxi_zip/", "true")
    taxiZipCount.show()
    val taxiYearCount = spark.sql("""
      SELECT YEAR(tpep_pickup_datetime) as year, count(*) AS taxi_count
      FROM taxi_table
      GROUP BY YEAR(tpep_pickup_datetime)
      ORDER BY YEAR(tpep_pickup_datetime) ASC NULLS LAST""")

    Utils.saveDataframe(taxiYearCount, "datasets/analytics/exploration/taxi_year/", "true")
    taxiYearCount.show()

    //    val taxiDateCount = spark.sql(
    //      """select %s , %s, count(*) from  taxi_table
    //     Group by %s, %s
    //     having %s is not null AND
    //      %s IN ("11371", "11430", "07114")""".stripMargin.format(NycTaxiDatasetSchemaMapped.pickupDateTime._1,
    //        NycTaxiDatasetSchemaMapped.PUZipCode._1,
    //        NycTaxiDatasetSchemaMapped.pickupDateTime._1,
    //        NycTaxiDatasetSchemaMapped.PUZipCode._1,
    //        NycTaxiDatasetSchemaMapped.PUZipCode._1,
    //        NycTaxiDatasetSchemaMapped.PUZipCode._1))
    //
    //    Utils.saveDataframe(taxiDateCount, "datasets/analytics/exploration/taxi_date_airport/", "true")

    /////// LYFT //////////

    val lyft_df = spark.read
      .format("csv")
      .schema(LyftDatasetSchemaCleaned.customSchema2)
      .option("header", "true")
      .load("datasets/analytics/lyft_join/")

    lyft_df.createOrReplaceTempView("lyft_table")

    val lyftZipCount = spark.sql("Select count(*) as lyft_count, zipcode as zip " +
      "FROM lyft_table " +
      "GROUP BY zipcode " +
      "ORDER BY zipcode ASC NULLS LAST"
    )

    Utils.saveDataframe(lyftZipCount, "datasets/analytics/exploration/lyft_zip/", "true")
    lyftZipCount.show()
    val lyftYearCount = spark.sql("""
      SELECT YEAR(%s) as year, count(*) AS lyft_count
      FROM lyft_table
      GROUP BY YEAR(%s)
      ORDER BY YEAR(%s) ASC NULLS LAST""".format(LyftDatasetSchemaCleaned.dateTime._1, LyftDatasetSchemaCleaned.dateTime._1
      , LyftDatasetSchemaCleaned.dateTime._1))

    Utils.saveDataframe(lyftYearCount, "datasets/analytics/exploration/lyft_year/", "true")
    lyftYearCount.show()
  }

}
