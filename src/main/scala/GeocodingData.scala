import datasetStructure.{LyftDatasetSchemaCleaned, LyftLatLongMappingSchema, NycTaxiDatasetSchemaCleaned, TaxiZoneLatLongMappingSchema, UberDatasetSchemaCleaned, UberLatLongMappingSchema}
import helper.Utils
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object GeocodingData {
  def appendZipCodeMappingToYellowTaxi(spark: SparkSession, taxiDf : sql.DataFrame): sql.DataFrame = {
    val taxiLatLongDf = Utils.convertRddToDf(spark, "datasets/yellow_taxi_join/", TaxiZoneLatLongMappingSchema.customSchema)

    val mappingTaxiDf = taxiLatLongDf.select(TaxiZoneLatLongMappingSchema.locationId, TaxiZoneLatLongMappingSchema.zipcode)

    taxiDf.createOrReplaceTempView("yellowTaxi")
    mappingTaxiDf.createOrReplaceTempView("mappingTaxi")

    val joinedPUTable = spark.sql("select yt.*, mt. " + TaxiZoneLatLongMappingSchema.zipcode + " as PUZipCode" +
      " FROM yellowTaxi yt" +
      " LEFT JOIN mappingTaxi mt" +
      " ON yt." + NycTaxiDatasetSchemaCleaned.PULocationID._1 + " = mt." + TaxiZoneLatLongMappingSchema.locationId)

//    joinedPUTable.printSchema()

    joinedPUTable.createOrReplaceTempView("joinedPUTable")

    val mappedYellowTaxiDf = spark.sql("select jput.*, mt." + TaxiZoneLatLongMappingSchema.zipcode + " as DOZipCode" +
      " FROM joinedPUTable jput" +
      " LEFT JOIN mappingTaxi mt" +
      " ON jput." + NycTaxiDatasetSchemaCleaned.DOLocationID._1 + " = mt." + TaxiZoneLatLongMappingSchema.locationId)

    mappedYellowTaxiDf
  }

  def appendZipCodeMappingToUber(spark: SparkSession, uberDf : sql.DataFrame): sql.DataFrame = {
    val uberLatLongDf = Utils.convertRddToDf(spark, "datasets/uber_data_mapping/", UberLatLongMappingSchema.customSchema)
//    val uberTaxiDf = uberLatLongDf.select(UberLatLongMappingSchema.lat, UberLatLongMappingSchema.long, UberLatLongMappingSchema.zipcode)

//    uberDf.createOrReplaceTempView("uber")
    uberLatLongDf.createOrReplaceTempView("uberLatLong")

    val uberTaxiLatLongSet = spark.sql(
      """select %s as %s,  %s as %s, round(%s, 4) as Lat, round(%s, 4) as Lon from uberLatLong where %s is not NULL""".stripMargin.format(
        UberLatLongMappingSchema.dateTime, UberDatasetSchemaCleaned.dateTime._1,
        UberLatLongMappingSchema.zipcode, UberDatasetSchemaCleaned.zipcode._1,
        UberLatLongMappingSchema.lat, UberLatLongMappingSchema.long, UberLatLongMappingSchema.zipcode))

//    uberTaxiLatLongSet.createOrReplaceTempView("uberTaxiLatLongSet")
//
//    val uberLatLongMappedDf = spark.sql("select u.*, ubl." + UberLatLongMappingSchema.zipcode + " as zipcode" +
//      " FROM uber u" +
//      " LEFT JOIN uberTaxiLatLongSet ubl" +
//      " ON u." + UberLatLongMappingSchema.lat + " = ubl." + UberLatLongMappingSchema.lat +
//      " AND u." + UberLatLongMappingSchema.long + " = ubl." + UberLatLongMappingSchema.long +
//      " where " + UberLatLongMappingSchema.zipcode + " is NOT NULL ")

    uberTaxiLatLongSet
  }

  def getMappedLyftData(spark: SparkSession): sql.DataFrame = {
    val lyftLatLongDf = Utils.convertRddToDf(spark, "datasets/lyft_data_mapping/", LyftLatLongMappingSchema.customSchema)
    //    val lyftTaxiDf = uberLatLongDf.select(UberLatLongMappingSchema.lat, UberLatLongMappingSchema.long, UberLatLongMappingSchema.zipcode)

    //    lyftDf.createOrReplaceTempView("uber")
    lyftLatLongDf.createOrReplaceTempView("lyftLatLong")

    val lyftTaxiLatLongSet = spark.sql(
      """select %s as %s,  %s as %s, round(%s, 4) as Lat, round(%s, 4) as Lon from lyftLatLong where %s is not NULL""".stripMargin.format(
        LyftLatLongMappingSchema.dateTime, LyftDatasetSchemaCleaned.dateTime._1,
        LyftLatLongMappingSchema.zipcode, LyftDatasetSchemaCleaned.zipcode._1,
        LyftLatLongMappingSchema.lat, LyftLatLongMappingSchema.long, LyftLatLongMappingSchema.zipcode))

    //    uberTaxiLatLongSet.createOrReplaceTempView("uberTaxiLatLongSet")
    //
    //    val uberLatLongMappedDf = spark.sql("select u.*, ubl." + UberLatLongMappingSchema.zipcode + " as zipcode" +
    //      " FROM uber u" +
    //      " LEFT JOIN uberTaxiLatLongSet ubl" +
    //      " ON u." + UberLatLongMappingSchema.lat + " = ubl." + UberLatLongMappingSchema.lat +
    //      " AND u." + UberLatLongMappingSchema.long + " = ubl." + UberLatLongMappingSchema.long +
    //      " where " + UberLatLongMappingSchema.zipcode + " is NOT NULL ")

    lyftTaxiLatLongSet
  }

 }
