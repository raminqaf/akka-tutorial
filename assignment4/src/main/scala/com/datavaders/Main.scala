package com.datavaders

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def time[R](block: => R): R = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      println(s"Execution: ${t1 - t0} ms")
      result
    }

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // TODO: Make folder name a parameter
    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"./TPCH/tpch_$name.csv")

    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}
