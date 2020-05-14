package datasetManagers.datasetCleaners

import java.time.LocalDateTime

import datasetStructure.{NycTaxiDatasetSchemaActual, NycTaxiDatasetSchemaCleaned}
import helper.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CleanTaxiDataset {
  val nycTaxiDatasetSchemaActual = NycTaxiDatasetSchemaActual
  val nycTaxiDatasetSchemaCleaned = NycTaxiDatasetSchemaCleaned

//  def cleanDateTimeColumn(inputDateTimeRdd: RDD[(String)]) : RDD[DateTime] = {
//    val dateTimeFormatter = new DateTimeFormatter("mm/dd/yyyy hh:mm:ss a")
//    val dateTimeRdd = inputDateTimeRdd.map(line => dateTimeFormatter.formateDate(line.toString))
//
//    val cleanedDateTimeRdd = dateTimeRdd.filter(line => line!=null)
//
//    cleanedDateTimeRdd
//  }

}
