package com.datavaders

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // Read a Dataset from a file
    val tables: ListBuffer[DataFrame] = ListBuffer()
    inputs.foreach(file_name => {
      tables += spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(file_name)
    })

    // DEBUG
    //    val inputs = List("artists", "tracks").map(name => s"./TPCH/$name.csv")
    //    val tables: ListBuffer[DataFrame] = ListBuffer()
    //    inputs.foreach(file_name => {
    //      tables += spark.read
    //        .option("inferSchema", "true")
    //        .option("header", "true")
    //        .option("delimiter", ",")
    //        .csv(file_name)
    //    })

    tables.foreach(table => {
      table.schema.names.foreach(column_name => {
        val keys = table.select(column_name).distinct().toDF("key")
        val columns = keys.withColumn("value", lit(column_name)).toDF()
        columns.show()
      })
    })
  }
}
