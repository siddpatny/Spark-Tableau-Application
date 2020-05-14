import Calulators.AnalyticsCalulators._
import datasetStructure._
import helper.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RunAnalytics {
  /** This Job is used to run analytics on the datasets. Joins are performed and the result analytics files are saved.
   * The Functions analytic functions are present in AnalyticsCalculators.scala
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val appName = "BigDataApplication_TaxisAndEventsNYC"
    val conf = new SparkConf().setMaster("local[30]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val outPutPath = new Path("datasets/analytics/")

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    val yellowTaxiDataframe =  Utils.convertRddToDf(spark, "datasets/cleanedTaxiDataset/yellow_taxi/", NycTaxiDatasetSchemaCleaned.customSchema)

    val uberDataframe =  Utils.convertRddToDf(spark, "datasets/cleanedTaxiDataset/uber/", UberDatasetSchemaCleaned.customSchema)
    val lyftDataframe =  Utils.convertRddToDf(spark, "datasets/cleanedTaxiDataset/lyft/", LyftDatasetSchemaCleaned.customSchema)

    val mappedYellowTaxiDf = GeocodingData.appendZipCodeMappingToYellowTaxi(spark, yellowTaxiDataframe)

    val uberMappedDf = GeocodingData.appendZipCodeMappingToUber(spark, uberDataframe)
    val lyftMappedDf = GeocodingData.getMappedLyftData(spark)

    yellowTaxiDataframe.createOrReplaceTempView("yellowTaxiDataframe")
    uberMappedDf.createOrReplaceTempView("uberMappedDf")
    lyftMappedDf.createOrReplaceTempView("lyftMappedDf")


    Utils.saveDataframeMultiple(mappedYellowTaxiDf, "datasets/analytics/yellow_taxi_join/")
    Utils.saveDataframeMultiple(uberMappedDf, "datasets/analytics/uber_join/")
    Utils.saveDataframeMultiple(lyftMappedDf, "datasets/analytics/lyft_join/")

    mappedYellowTaxiDf.createOrReplaceTempView("mappedYellowTaxi")

    val yellowTaxiDf = spark.sql("Select count(*) as pu_count, PUZipCode " +
      " FROM mappedYellowTaxi" +
      " GROUP BY PUZipCode") //.write.option("header", "true").format("csv").save("datasets/analytics/yellow_taxi/PUZipcodeCount")

    yellowTaxiDf.createOrReplaceTempView("grouped_yellow_taxi")

    val businessDf = Utils.convertRddToDf(spark, "datasets/cleanedEBDataset/csv_format/business/", BusinessDatasetSchema.customSchema)

    businessDf.createOrReplaceTempView("business_table")

    val businessCountDf = spark.sql("Select count(*) as business_count, zipcode as business_zipcode " +
      "FROM business_table " +
      "WHERE status=\"Active\"" +
      "GROUP BY zipcode ")

    businessCountDf.createOrReplaceTempView("grouped_business_table")

    val joinedBusinessYellowTaxi = spark.sql("select * " +
      " FROM grouped_yellow_taxi gyt " +
      " LEFT JOIN grouped_business_table gbt " +
      " ON gyt.PUZipCode = gbt.business_zipcode")

    println(joinedBusinessYellowTaxi.first())

    Utils.saveDataframe(joinedBusinessYellowTaxi, "datasets/analytics/yellow_taxi/PUZipcodeCount", "true")

//    joinedBusinessYellowTaxi.show()

    event_address_join(spark)
    println("event_address_join done")
//    getTimeSeries(spark)
    println("timeseries done")
//    joinBusinessTaxi(spark)
    println("completed events join, timeseries and join business.")
    val taxiGroupByAirports = getTaxiGroupByAirports(spark)
    val airportsGroupedByAirports = getAirportsGroupByAirports(spark)

    getYearAndZipCounts(spark)

    println("\n\nAnalytics completed")

//    getTopEventsWithTaxi()

  }
}
