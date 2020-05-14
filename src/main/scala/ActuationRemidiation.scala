import helper.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ActuationRemidiation {
  /** Give user insights to the data using various switches
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\")
    val appName = "BigDataApplication_TaxisAndEventsNYC"
    val conf = new SparkConf().setMaster("local[50]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val options = Utils.parseArguments(args)

    if(!options.contains('acre)){
      return
    }

    if(options.contains('event)) {
      if (options.contains('event_zip)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/events_zip")
        val s = "zip = '%s'".format(options.getOrElse('event_zip, null))
        println("------ Events at your Zip Code ------")
        event_df.select("event_count").where(s).collect.foreach(println)
      }

      if (options.contains('event_year)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/events_year")
        val s = "year = '%s'".format(options.getOrElse('event_year, null))
        println("------ Events at your year ------")
        event_df.select("event_count").where(s).collect.foreach(println)
      }
    }

    if(options.contains('business)) {
      if (options.contains('b_zip)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/business_zip")
        val s = "zip = '%s'".format(options.getOrElse('b_zip, null))
        println("------ Active Businesses at your Zip Code ------")
        event_df.select("business_count").where(s).collect.foreach(println)
      }

      if (options.contains('b_year)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/business_year")
        val s = "year = '%s'".format(options.getOrElse('b_year, null))
        println("------ Businesses ended this year ------")
        event_df.select("business_count").where(s).collect.foreach(println)
      }
    }

    if(options.contains('y_taxi)) {
      if (options.contains('y_taxi_zip)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/taxi_zip")
        val s = "zip = '%s'".format(options.getOrElse('y_taxi_zip, null))
        println("------ Taxis at your Zip Code ------")
        event_df.select("taxi_count").where(s).collect.foreach(println)
      }

      if (options.contains('y_taxi_year)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/taxi_year")
        val s = "year = '%s'".format(options.getOrElse('y_taxi_year, null))
        println("------ Taxis this year ------")
        event_df.select("taxi_count").where(s).collect.foreach(println)
      }
    }

    if(options.contains('uber_taxi)) {
      if (options.contains('uber_taxi_zip)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/uber_zip")
        val s = "zip = '%s'".format(options.getOrElse('uber_taxi_zip, null))
        println("------ Number of uber in your Zip Code ------")
        event_df.select("uber_count").where(s).collect.foreach(println)
      }
    }

    if(options.contains('lyft_taxi)) {
      if (options.contains('lyft_taxi_zip)) {
        val event_df = Utils.readDataframe(spark, "datasets/analytics/exploration/lyft_zip")
        val s = "zip = '%s'".format(options.getOrElse('lyft_taxi_zip, null))
        println("------ Number of lyft in your Zip Code ------")
        event_df.select("lyft_count").where(s).collect.foreach(println)
      }
    }

    if(options.contains('corr_taxi_ap)) {
      println("------ Correlation of Taxi count with number of flights for every month (in asc order) for the year 2018 ------")
      val pu_flight_count = Utils.readDataframe(spark, "datasets/analytics/comparision/pu_flight_count")
      pu_flight_count.show()
      val do_flight_count = Utils.readDataframe(spark, "datasets/analytics/comparision/do_flight_count")
      do_flight_count.show()
    }

    if(options.contains('corr_taxi_ap)) {
      println("------ Correlation of Passenger count with number of flights for every month (in asc order) for the year 2018 ------")
      val pu_flight_pass_count = Utils.readDataframe(spark, "datasets/analytics/comparision/pu_flight_passenger")
      pu_flight_pass_count.show()
      val do_flight_pass_count = Utils.readDataframe(spark, "datasets/analytics/comparision/do_flight_passenger")
      do_flight_pass_count.show()
    }

  }
}
