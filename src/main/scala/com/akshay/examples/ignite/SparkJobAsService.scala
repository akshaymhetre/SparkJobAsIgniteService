package com.akshay.examples.ignite

import org.apache.ignite.services.{Service, ServiceContext}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

trait SparkService extends Service {
  def run
}

class SparkJobAsService extends SparkService {
  override def cancel(serviceContext: ServiceContext): Unit = println("Cancelled Job")

  override def init(serviceContext: ServiceContext): Unit = println("Initializing Job")

  override def execute(serviceContext: ServiceContext): Unit = {
    println("Executing Job As a service")
  }

  def run = {
    println("Executing Spark Job")
    val spark = SparkSession
      .builder()
      .appName("SparkJobAsService")
      .master("local")
      .getOrCreate()

    Logger.getRootLogger.setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.ignite").setLevel(Level.DEBUG)

    val path = getClass().getResource("/cache.xml").getPath

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val ds = spark.sqlContext.createDataset(1 to 10).withColumn("id", monotonically_increasing_id)

    println("************* Data To be save in Ignite *******************")

    ds.show(2)

    println("Saving in ignite")

    ds.write
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, path)
      .option(OPTION_TABLE, "SAMPLE")
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
      .option(OPTION_CREATE_TABLE_PARAMETERS, "template=partitioned")
      .mode(SaveMode.Overwrite)
      .save()

    println("Saved in ignite")

    val ds1 = spark.read
      .format(FORMAT_IGNITE)
      .option(OPTION_CONFIG_FILE, path)
      .option(OPTION_TABLE, "SAMPLE")
      .load()

    println("************* Data Loaded from Ignite *******************")

    ds1.show(2)

    println("Closing Spark Session")
    spark.close()
    println("Closed Spark Session")
  }
}
