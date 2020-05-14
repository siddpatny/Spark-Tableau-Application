package datasetManagers.datasetCleaners

import java.time.LocalDateTime

import datasetStructure.{LyftDatasetSchemaActual, LyftDatasetSchemaCleaned}
import helper.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CleanLyftDataset {
//  def cleanDateTimeColumn(inputDateTimeRdd: RDD[(String)]) = {
//    val dateTimeFormatter = new DateTimeFormatter("mm/dd/yyyy HH:mm")
//    val dateTimeRdd = inputDateTimeRdd.map(line => dateTimeFormatter.formateDate(line.toString))
//
//    val cleanedDateTimeRdd = dateTimeRdd.filter(line => line!=null)
//
//    cleanedDateTimeRdd
//  }

}
