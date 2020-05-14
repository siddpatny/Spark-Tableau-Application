import datasetManagers.datasetCleaners.GeocodeAddressBing
import datasetManagers.{EBDatasetManager, FlightDatasetManager, LyftDatasetManager, TaxiDatasetManager, UberDatasetManager}
import helper.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Cleaning {
  /** Cleaning irrelevant records from all datasets.
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val appName = "BigDataApplication_TaxisAndEventsNYC"
    val conf = new SparkConf().setMaster("yarn").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val options = Utils.parseArguments(args)

    var runGeocode = false

    val bingKey = options.getOrElse('bingKey, null)
    if(bingKey != null){
      runGeocode = options.contains('geocode)
    }

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val outputPath_business = new Path("datasets/cleanedEBDataset/")
    val outputPath_taxi = new Path("datasets/cleanedTaxiDataset/")
    val outputPath_flight = new Path("datasets/cleanedFlightDataset/")


    if (fs.exists(outputPath_business))
      fs.delete(outputPath_business, true)

    if (fs.exists(outputPath_taxi))
      fs.delete(outputPath_taxi, true)


    if (fs.exists(outputPath_flight))
      fs.delete(outputPath_flight, true)

    val taxiDatasetFileName = "datasets/yellow_taxi_historical/*.csv"
    val uberDatasetFileName = "datasets/uber/*.csv"
    val lyftDatasetFileName = "datasets/lyft/*.csv"
    val eventDatasetFileName = "datasets/events/*.csv"
    val businessDatasetFileName = "datasets/business/*.csv"
    val flightDatasetFileName = "datasets/airports/*.csv"

    // Cleaning

    //Taxi Datasets
    val taxiDatasetRdd = TaxiDatasetManager.cleanDataset(taxiDatasetFileName, spark, sc)
    val uberDatasetRdd = UberDatasetManager.cleanDataset(uberDatasetFileName, spark, sc)
    val lyftDatasetRdd = LyftDatasetManager.cleanDataset(lyftDatasetFileName, spark, sc)

    val taxiDatasetCsvRdd = Utils.reformatRddToSave(taxiDatasetRdd._1, taxiDatasetRdd._2, sc)
    val uberDatasetCsvRdd = Utils.reformatRddToSave(uberDatasetRdd._1, uberDatasetRdd._2, sc)
    val lyftDatasetCsvRdd = Utils.reformatRddToSave(lyftDatasetRdd._1, lyftDatasetRdd._2, sc)

    taxiDatasetCsvRdd.saveAsTextFile(outputPath_taxi + "/yellow_taxi/")
    uberDatasetCsvRdd.saveAsTextFile(outputPath_taxi + "/uber/")
    lyftDatasetCsvRdd.saveAsTextFile(outputPath_taxi + "/lyft/")

//    Utils.saveTaxiRddToCsv(sc, spark, taxiDatasetRdd)
    Utils.saveUberRddToCsv(sc, spark, uberDatasetRdd, "/uber")
    Utils.saveUberRddToCsv(sc, spark, lyftDatasetRdd, "/lyft")

    //Events Dataset
    val eventDatasetRDD = EBDatasetManager.cleanEvents(eventDatasetFileName,spark,sc)
    val businessDatasetRDD = EBDatasetManager.cleanBusiness(businessDatasetFileName,spark,sc)

    Utils.saveEventRddToCsv(spark, eventDatasetRDD)
    Utils.saveBusinessRddToCsv(spark,businessDatasetRDD)


    //Flight Dataset

    val flightDatasetRDD = FlightDatasetManager.cleanDataset(flightDatasetFileName,spark,sc)

    Utils.saveDataframeMultiple(flightDatasetRDD._2, outputPath_flight + "/nyc_airports/")

    if(runGeocode) {
      if (fs.exists(new Path ("datasets/events_address_mapping/")))
        fs.delete(new Path ("datasets/events_address_mapping/"), true)

      val geocodingDataMapping = GeocodeAddressBing.geocodeMap(sc, bingKey.toString)
      geocodingDataMapping.saveAsTextFile("datasets/events_address_mapping/")
    }
    println("\n\nCleaning Yellow_taxi, Uber and Lyft data completed")
  }
}
