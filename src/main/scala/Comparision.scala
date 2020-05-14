import Calulators.ComparisionCalculator._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Comparision {
  /** This Job is used to run Comparision on the datasets. Files are picked from Analytics to run mathematical operations
   * to further analyse the data.
   * The Functions Comparision are present in ComparisionCalculators.scala
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val appName = "BigDataApplication_TaxisAndEventsNYC"
    val conf = new SparkConf().setMaster("local[10]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val fs = FileSystem.get(sc.hadoopConfiguration)

    val outPutPath = new Path("datasets/analytics/comparision/")

    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)

    getAirportPickUpTaxiCorrMonth(sc, spark)
    println("pickup corr done")
    getAirportDropOffTaxiCorrMonth(sc, spark)
    println("dropOff corr done")

    getTopEventsAndBusinessesWithTaxi(spark)
    println("getTopEventsAndBusinessesWithTaxi done")

  }
}
