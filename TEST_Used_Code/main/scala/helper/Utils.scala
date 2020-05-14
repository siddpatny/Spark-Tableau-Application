package helper

import java.time.LocalDateTime

import datasetStructure.{Business, Event, FlightDatasetSchemaCleaned}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkContext, sql}
import org.apache.hadoop.fs._

import scala.collection.mutable.ListBuffer

object Utils {

	def toCsvLine(data: String): String = {
		data.substring(1, data.length-1)
	}

  def reformatRddToSave[T](headers: Array[String], rdd: RDD[T], sc: SparkContext): RDD[String] = {
    sc.makeRDD(Array(headers.mkString(", "))).union(rdd.map(line => Utils.toCsvLine(line.toString)))
  }


  // saving cleaned data to csv for geocoding
  def saveTaxiRddToCsv(sc: SparkContext, spark: SparkSession, rdd: (Array[String], RDD[(Int, LocalDateTime, LocalDateTime, Int, Int, Int)])) = {
    val basePath = "datasets/cleanedTaxiDataset/csv_format/yellow_taxi/"
    val saveRdd = rdd._2.map(line => (line._1, line._2.toString, line._3.toString, line._4, line._5, line._6))
    spark.createDataFrame(saveRdd).toDF(rdd._1: _*).repartition(1).write.format("csv").option("header", "true").save(basePath)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileName = fs.globStatus(new Path(basePath + "part*"))(0).getPath.getName
    fs.rename(new Path(basePath + fileName), new Path(basePath + "cleaned_combined_yellow_taxi.csv"))
  }

  def saveUberRddToCsv(sc: SparkContext, spark: SparkSession, rdd: (Array[String], RDD[(LocalDateTime, Float, Float)]), folderName : String) = {
    val basePath = "datasets/cleanedTaxiDataset/csv_format/" + folderName + "/"
    val saveRdd = rdd._2.map(line => (line._1.toString, line._2, line._3))
    spark.createDataFrame(saveRdd).toDF(rdd._1: _*).repartition(1).write.format("csv").option("header", "true").save(basePath)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileName = fs.globStatus(new Path(basePath + "/part*"))(0).getPath.getName
    fs.rename(new Path(basePath + fileName), new Path(basePath + "cleaned_combined_" + folderName + ".csv"))
  }

	def saveEventRddToCsv(spark: SparkSession, rdd: (Array[String], RDD[Event])) = {
		val saveRdd = rdd._2.map(line => (line.zipcode, line.borough, line.address, line.end_date.toString, line.start_date
      .toString, line.e_type, line.id.toString))
		spark.createDataFrame(saveRdd).toDF(rdd._1: _*).repartition(1).write.format("csv").option("header", "true").save("datasets/cleanedEBDataset/csv_format/events")
	}

	def saveBusinessRddToCsv(spark: SparkSession, rdd: (Array[String], RDD[Business])) = {
		val saveRdd = rdd._2.map(line => (line.zipcode, line.state, line.city, line.address, line.end_date.toString, line.start_date.toString, line.status, line.b_type))
		spark.createDataFrame(saveRdd).toDF(rdd._1: _*).repartition(1).write.format("csv").option("header", "true").save("datasets/cleanedEBDataset/csv_format/business")
	}

  def convertRddToDf(spark: SparkSession, folderName: String, schema : StructType) : sql.DataFrame = {
    spark.read.format("csv").schema(schema).option("header", "true").load(folderName)
  }

  def saveDataframe(df : sql.DataFrame, path: String, header : String): Unit = {
    df.repartition(1).write.option("header", header).format("csv").save(path)
  }

  def saveDataframeMultiple(df : sql.DataFrame, path: String): Unit = {
    df.write.option("header", "true").format("csv").save(path)
  }

  def parseArguments(args: Array[String]) = {
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "-geocode" :: tail =>
          nextOption(map ++ Map('geocode -> true), tail)
        case "-bingKey" :: opt2 :: tail =>
          nextOption(map ++ Map('bingKey -> opt2), list.tail)
        case "-acre" :: tail =>
          nextOption(map ++ Map('acre -> true), tail)
        case "-event" :: tail =>
          nextOption(map ++ Map('event -> true), tail)
        case "-event_zip" :: opt2 :: tail =>
          nextOption(map ++ Map('event_zip -> opt2), list.tail)
        case "-event_year" :: opt2 :: tail =>
          nextOption(map ++ Map('event_year -> opt2), list.tail)
        case "-business" :: tail =>
          nextOption(map ++ Map('business -> true), tail)
        case "-b_zip" :: opt2 :: tail =>
          nextOption(map ++ Map('b_zip -> opt2), list.tail)
        case "-b_year" :: opt2 :: tail =>
          nextOption(map ++ Map('b_year -> opt2), list.tail)
        case "-y_taxi" :: tail =>
          nextOption(map ++ Map('y_taxi -> true), tail)
        case "-y_taxi_zip" :: opt2 :: tail =>
          nextOption(map ++ Map('y_taxi_zip -> opt2), list.tail)
        case "-y_taxi_year" :: opt2 :: tail =>
          nextOption(map ++ Map('y_taxi_year -> opt2), list.tail)
        case "-uber_taxi" :: tail =>
          nextOption(map ++ Map('uber_taxi -> true), tail)
        case "-uber_taxi_zip" :: opt2 :: tail =>
          nextOption(map ++ Map('uber_taxi_zip -> opt2), list.tail)
        case "-lyft_taxi" :: tail =>
          nextOption(map ++ Map('lyft_taxi -> true), tail)
        case "-lyft_taxi_zip" :: opt2 :: tail =>
          nextOption(map ++ Map('lyft_taxi_zip -> opt2), list.tail)
        case default => nextOption(map , list.tail)
      }
    }
    nextOption(Map(),arglist)
  }

  def readDataframe(spark: SparkSession, filePath : String): sql.DataFrame ={
    spark.read.format("csv").option("header", "true").load(filePath)
  }

  def getCorrelation(df: sql.DataFrame, col1: String, col2 : String) = {
    df.stat.corr(col1, col2)
  }

  def convert2DListToDf(sc: SparkContext, spark: SparkSession, list: List[List[Double]], schema : StructType) = {
    val corrRdd = sc.parallelize(list).map(Row.fromSeq(_))
    spark.createDataFrame(corrRdd, schema)
  }
}
