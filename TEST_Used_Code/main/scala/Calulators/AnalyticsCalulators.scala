package Calulators

import datasetStructure.{AddressMappingSchema, BusinessDatasetSchema, EventsDatasetSchema, FlightDatasetSchemaCleaned, LyftDatasetSchemaCleaned, NycTaxiDatasetSchemaCleaned, NycTaxiDatasetSchemaMapped, UberDatasetSchemaCleaned}
import helper.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, mean, stddev, when}
import org.apache.spark.storage.StorageLevel

object AnalyticsCalulators {

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

}
