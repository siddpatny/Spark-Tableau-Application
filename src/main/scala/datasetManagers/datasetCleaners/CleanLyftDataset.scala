package datasetManagers.datasetCleaners

import java.time.LocalDateTime

import datasetStructure.{LyftDatasetSchemaActual, LyftDatasetSchemaCleaned}
import helper.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CleanLyftDataset {
  def cleanDataset(lyftDatasetFileName: String, spark: SparkSession, sc: SparkContext): (Array[String], RDD[(LocalDateTime, Float, Float)]) ={
    val dataset = sc.textFile(lyftDatasetFileName)

    val lyftDataset= dataset.map(line=> line.split(","))

    val csvHeader = lyftDataset.first()

    val taxiDatasetRdd = lyftDataset.mapPartitionsWithIndex {
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    }

//    println(taxiDatasetRdd.count())

    val dateTimeFormatter = new DateTimeFormatter("M/d/yyyy H:m")
    val filteredDataset = taxiDatasetRdd.map(line => cleanData(line, dateTimeFormatter))
    val cleanDataset = filteredDataset.filter(line => line!= null)
//    println(cleanDataset.count())

    (LyftDatasetSchemaCleaned.getHeaders, cleanDataset)
  }

  def cleanData(line: Array[String], dateTimeFormatter: DateTimeFormatter): (LocalDateTime, Float, Float) ={
     try {
       (dateTimeFormatter.formateDate(line(LyftDatasetSchemaActual.datetime._2)), line(LyftDatasetSchemaActual.lat._2).toFloat, line(LyftDatasetSchemaActual.long._2).toFloat)
     } catch {
       case _: Exception => null
     }
  }
}
