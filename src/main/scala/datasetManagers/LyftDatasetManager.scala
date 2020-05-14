package datasetManagers

import java.time.LocalDateTime

import datasetManagers.datasetCleaners.CleanLyftDataset
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object LyftDatasetManager {

  def profileDataset(taxiDatasetFileName: String, spark: SparkSession, sc: SparkContext): List[String] ={

    val dataset = sc.textFile(taxiDatasetFileName)

    val taxiDataset= dataset.map(line=> line.split(","))

    var profileList = List.empty[String]

    profileList = profileList :+ "Number of Rows in Original Lyft Dataset = "+ taxiDataset.count().toString

    val csvHeader = taxiDataset.first()

    profileList :+= "Headers in original Lyft Dataset = "+ csvHeader.mkString(", ")

    val cleanRdd = cleanDataset(taxiDatasetFileName, spark, sc)


    profileList :+= "Number of Rows in Cleaned Lyft Dataset = "+ (cleanRdd._2.count() + 1)
    profileList :+= "Headers in cleaned Lyft Dataset = "+ cleanRdd._1.mkString(", ")

    val monthCount = cleanRdd._2.map(line => (line._1.getMonth, 1)).reduceByKey(_ + _).collect()
    monthCount.map(line => profileList :+= ("Number of Lyft trips in " + line._1 + " = " + line._2))

    profileList
  }

  def cleanDataset(lyftDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], RDD[(LocalDateTime, Float, Float)]) ={
    CleanLyftDataset.cleanDataset(lyftDatasetFileName, spark, sc)
  }
}
