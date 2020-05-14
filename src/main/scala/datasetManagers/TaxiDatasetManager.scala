package datasetManagers

import java.time.LocalDateTime

import datasetManagers.datasetCleaners.CleanTaxiDataset
import datasetStructure.NycTaxiDatasetSchemaActual
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TaxiDatasetManager {
  val nycTaxiDatasetSchemaActual = NycTaxiDatasetSchemaActual

  def profileDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] ={

    val dataset = sc.textFile(taxiDatasetFileName)
    val taxiDataset= dataset.map(line=> line.split(","))
    var profileList = List.empty[String]
    val csvHeader = taxiDataset.first()
    profileList = profileList :+ "Number of Rows in Original Yellow Taxi Dataset = "+ taxiDataset.count().toString
    profileList :+= "Headers in Yellow Taxi Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanDataset(taxiDatasetFileName, spark, sc)

    profileList :+= "Number of Rows in Cleaned Yellow Taxi Dataset = "+ (cleanRdd._2.count() + 1)
    profileList :+= "Headers in cleaned Yellow Taxi Dataset = "+ cleanRdd._1.mkString(", ")

    val puMonthCount = cleanRdd._2.map(line => (line._2.getMonth, 1)).reduceByKey(_ + _).collect()
    puMonthCount.map(line => profileList :+= ("Number of Yellow Taxi Pickups in " + line._1 + " = " + line._2))

    val doMmonthCount = cleanRdd._2.map(line => (line._3.getMonth, 1)).reduceByKey(_ + _).collect()
    doMmonthCount.map(line => profileList :+= ("Number of Yellow Taxi drops in " + line._1 + " = " + line._2))

    profileList
  }

  def cleanDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext):  (Array[String], RDD[(Int, LocalDateTime, LocalDateTime, Int, Int, Int)]) ={
    CleanTaxiDataset.cleanDataset(taxiDatasetFileName, spark, sc)
  }

}
