import datasetStructure.{LyftDatasetSchemaCleaned, LyftLatLongMappingSchema, NycTaxiDatasetSchemaCleaned, TaxiZoneLatLongMappingSchema, UberDatasetSchemaCleaned, UberLatLongMappingSchema}
import helper.Utils
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object GeocodingData {

  def appendZipCodeMappingToUber(spark: SparkSession, uberDf : sql.DataFrame): sql.DataFrame = {
    val uberTaxiLatLongSet = uberLatLongDf.select(UberLatLongMappingSchema.lat, UberLatLongMappingSchema.long, UberLatLongMappingSchema.zipcode)

    uberDf.createOrReplaceTempView("uber")

    uberTaxiLatLongSet.createOrReplaceTempView("uberTaxiLatLongSet")

    val uberLatLongMappedDf = spark.sql("select u.*, ubl." + UberLatLongMappingSchema.zipcode + " as zipcode" +
      " FROM uber u" +
      " LEFT JOIN uberTaxiLatLongSet ubl" +
      " ON u." + UberLatLongMappingSchema.lat + " = ubl." + UberLatLongMappingSchema.lat +
      " AND u." + UberLatLongMappingSchema.long + " = ubl." + UberLatLongMappingSchema.long +
      " where " + UberLatLongMappingSchema.zipcode + " is NOT NULL ")

    uberTaxiLatLongSet
  }

  def getMappedLyftData(spark: SparkSession): sql.DataFrame = {
        val lyftTaxiLatLongSet = uberLatLongDf.select(UberLatLongMappingSchema.lat, UberLatLongMappingSchema.long, UberLatLongMappingSchema.zipcode)

        lyftDf.createOrReplaceTempView("uber")


        uberTaxiLatLongSet.createOrReplaceTempView("uberTaxiLatLongSet")

        val uberLatLongMappedDf = spark.sql("select u.*, ubl." + UberLatLongMappingSchema.zipcode + " as zipcode" +
          " FROM uber u" +
          " LEFT JOIN uberTaxiLatLongSet ubl" +
          " ON u." + UberLatLongMappingSchema.lat + " = ubl." + UberLatLongMappingSchema.lat +
          " AND u." + UberLatLongMappingSchema.long + " = ubl." + UberLatLongMappingSchema.long +
          " where " + UberLatLongMappingSchema.zipcode + " is NOT NULL ")

    lyftTaxiLatLongSet
  }

 }
