package com.datavaders

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

object Sindy {
  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._
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
//        val inputs = List("artists", "tracks").map(name => s"./TPCH/$name.csv")
//        val tables: ListBuffer[DataFrame] = ListBuffer()
//        inputs.foreach(file_name => {
//          tables += spark.read
//            .option("inferSchema", "true")
//            .option("header", "true")
//            .option("delimiter", ",")
//            .csv(file_name)
//        })

    // Produce flattened and unionized columns dataframe
    var flatenedColumns = spark.emptyDataFrame.withColumn("key", lit("")).withColumn("value", lit(""))
    tables.foreach(table => {
      table.schema.names.foreach(column_name => {
        val keys = table.select(column_name).distinct().toDF("key")
        val columns = keys.withColumn("value", lit(column_name)).toDF()
        flatenedColumns = flatenedColumns.union(columns)
      })
    })

    flatenedColumns.as[(String, String)]
      // Group data frame by data entries (key = data entry; value = column name) (Can't user show after that)
      .groupByKey{ case (key, value) => key}
      // Remove the column with the data entries (so that we only have the column lists)
      .mapGroups { case(group, values) => values.map(value => value._2).toList}
      // Remove duplicated column list entries
      .distinct()
      // For each row with column lists explode all values and add the original list as a second column to it
      .withColumn("value_exploded", explode($"value"))
      .as[(List[String], String)]
      // For each column we want to see the collected column lists (Can't user show after that)
      .groupByKey { case (list, value) => value }
      // For columns where we have an inclusion dependency return this list. For others return null
      .mapGroups{ case(group, values) => {
        val valuesList = values.toList
        var returnValue : (List[String], String) = null
        if(valuesList.length == 1 && valuesList.head._1.length > 1) {
          returnValue = (valuesList.head._1, group)
        } else {
          returnValue = (null, group)
        }
        returnValue
      }}
      // Sorting can't happen at the end because the ouput gets shuffled (TODO: Probably something with stages)
      .sort("_2")
      // Remove unsuitable candidates
      .filter(row => row._1 != null)
      // Filter column name out of inclusion list (for some reason it didn't worked if we do it before the filtering)
      .map(row => (row._1.filterNot(x => x.equals(row._2)).sorted, row._2))
      // Print the output
      .foreach( row => {
        print(row._2 + "  < ")
        print(row._1.head)
        row._1.splitAt(1)._2.foreach( string => print(", " + string))
        println()
      })
//      .show(false)



    //    println(spark.emptyDataFrame.withColumn("test", explode(($"value").as("value"))).show())
//    reducedCols.mapGroups((key, valueIterator) => {
//      var strings: ListBuffer[String] = ListBuffer()
//      valueIterator.foreach(value => {
//        strings += value.getString(0) })
//      return strings
//      }).show()
  }
}
