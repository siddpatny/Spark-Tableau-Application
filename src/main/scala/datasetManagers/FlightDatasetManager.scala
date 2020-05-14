package datasetManagers

import datasetManagers.LyftDatasetManager.cleanDataset
import datasetStructure.FlightDatasetSchemaCleaned
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FlightDatasetManager {
  def cleanDataset(airportDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], sql.DataFrame) = {
    val airport_df = spark.read.format("csv").option("header", "true").load(airportDatasetFileName)

    airport_df.createOrReplaceTempView("airport_table")

    val dest_filtered_airport_df = spark.sql("SELECT %s FROM airport_table WHERE DEST in ('EWR','JFK','LGA') OR ORIGIN in ('EWR','JFK','LGA') AND FL_DATE IS NOT NULL".format(FlightDatasetSchemaCleaned.getHeaders.mkString(",")))

    val new_cols_df = dest_filtered_airport_df.withColumn(FlightDatasetSchemaCleaned.taxi_zone_dest, {when(col("DEST").equalTo("EWR"), 1).otherwise({
      when(col("DEST").equalTo("JFK"), 132).otherwise({
        when(col("DEST").equalTo("LGA"), 138).otherwise(null)
      })
    })}).
      withColumn(FlightDatasetSchemaCleaned.taxi_zone_orig, {when(col("ORIGIN").equalTo("EWR"), 1).otherwise({
      when(col("ORIGIN").equalTo("JFK"), 132).otherwise({
        when(col("ORIGIN").equalTo("LGA"), 138).otherwise(null)
      })
    })})

    (FlightDatasetSchemaCleaned.getHeaders, new_cols_df)
  }

  def profileDataset(airportDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] = {
    val dataset = sc.textFile(airportDatasetFileName)

    val airportDataset= dataset.map(line=> line.split(","))

    var profileList = List.empty[String]

    profileList = profileList :+ "Number of Rows in Original Airport Dataset = "+ airportDataset.count().toString

    val csvHeader = airportDataset.first()

    profileList :+= "Headers in original Airport Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanDataset(airportDatasetFileName, spark, sc)

    profileList :+= "Number of Rows in Cleaned Airport Dataset = "+ (cleanRdd._2.rdd.count() + 1)
    profileList :+= "Headers in cleaned Airport Dataset = "+ cleanRdd._1.mkString(", ")

    profileList
  }

}
