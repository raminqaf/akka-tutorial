package com.datavaders

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    var path = "./TPCH"
    var cores = 4
    if (args.length == 4) {
      var argsList = args.toList
      while (argsList.nonEmpty)
        argsList match {
          case "--path" :: head :: tail =>
            path = head
            argsList = tail
          case "--cores" :: head :: tail =>
            cores = head.toInt
            argsList = tail
        }
    }

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
      .master(s"local[$cores]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    val partitions = cores * 2
    spark.conf.set("spark.sql.shuffle.partitions", s"$partitions") //

    // TODO: Make folder name a parameter
    val inputs = List("region", "nation", "supplier", "customer", "part", "lineitem", "orders")
      .map(name => s"$path/tpch_$name.csv")

    time {
      Sindy.discoverINDs(inputs, spark)
    }
  }
}
