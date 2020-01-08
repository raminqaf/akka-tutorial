package com.datavaders

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

import scala.collection.mutable.ListBuffer

object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {



    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // TODO: Make folder name a parameter
    // Read a Dataset from a file
    val tables: ListBuffer[DataFrame] = ListBuffer()
    inputs.foreach( file_name => {
      tables += spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(file_name)
    })


    val columns = spark.emptyDataFrame
    tables.foreach(table => {
      table.schema.names.foreach( column_name => {
        val df = spark.emptyDataFrame
        df.withColumn("key", collect_set(table.col(column_name)))
        size
        df.withColumn("value", typedLit(List.fill(size(collect_set(table.col(column_name)))))(column_name))
        columns.union(df)
        collect_set(table.col(column_name))
      })
    })

  }
}
