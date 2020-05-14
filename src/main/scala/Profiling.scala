import datasetManagers.{EBDatasetManager, FlightDatasetManager, LyftDatasetManager, TaxiDatasetManager, UberDatasetManager}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Profiling {
  /** This Job is used to profiling of the datasets. Joins are performed and the result file is saved.
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

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val outPutPath = new Path("datasets/profiling/")

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    val taxiDatasetFileName = "datasets/yellow_taxi_historical/"
    val uberDatasetFileName = "datasets/uber/"
    val lyftDatasetFileName = "datasets/lyft/"
    val eventDatasetFileName = "datasets/events/"
    val businessDatasetFileName = "datasets/business/"
    val airportsDatasetFileName = "datasets/airports/"

    // Profiling

    //Taxi
    val yellowTaxiProfileList = TaxiDatasetManager.profileDataset(taxiDatasetFileName, spark, sc)
    val uberTaxiProfileList = UberDatasetManager.profileDataset(uberDatasetFileName, spark, sc)
    val lyftTaxiProfileList = LyftDatasetManager.profileDataset(lyftDatasetFileName, spark, sc)

    // events
    val eventProfileList = EBDatasetManager.profileEvents(eventDatasetFileName,spark,sc)
    val businessProfileList = EBDatasetManager.profileBusiness(businessDatasetFileName,spark,sc)


    // Flights
    val flightsProfileList = FlightDatasetManager.profileDataset(airportsDatasetFileName,spark,sc)

    // Appending all profiles
    val dataProfile = yellowTaxiProfileList ::: uberTaxiProfileList ::: lyftTaxiProfileList :: eventProfileList :: businessProfileList :: flightsProfileList
    dataProfile.foreach(println)
    val profileRdd = sc.parallelize(dataProfile)
    profileRdd.saveAsTextFile("datasets/profiling/")
    println("\n\nProfiling Yellow_taxi, Uber and Lyft data completed")
  }
}
