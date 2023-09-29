/*
 * Copyright 2015 Databricks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

case class GenTPCDSDataConfig(
    master: String = "local[*]",
    dsdgenDir: String = null,
    dsdgenDistArchive: String = "",
    scaleFactor: String = null,
    location: String = null,
    format: String = null,
    useDoubleForDecimal: Boolean = false,
    useStringForDate: Boolean = false,
    overwrite: Boolean = false,
    partitionTables: Boolean = true,
    clusterByPartitionColumns: Boolean = true,
    filterOutNullPartitionValues: Boolean = true,
    tableFilter: String = "",
    numPartitions: Int = 100,
    databaseName: String = "",
    dropExistingDatabase: Boolean = false,
    analyzeTables: Boolean = false,
    copyToDatabase: Boolean = true)

/**
 * Gen TPCDS data.
 * To run this:
 * {{{
 *   build/sbt "test:runMain <this class> -d <dsdgenDir> -s <scaleFactor> -l <location> -f <format>"
 * }}}
 */
object GenTPCDSData {
  private val log = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenTPCDSDataConfig]("Gen-TPC-DS-data") {
      opt[String]('m', "master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]('d', "dsdgenDir")
        .action { (x, c) => c.copy(dsdgenDir = x) }
        .text("location of dsdgen")
        .required()
      opt[String]('a', "dsdgenDistArchive")
        .action { (x, c) => c.copy(dsdgenDistArchive = x) }
        .text("local archive with dsddgen")
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
      opt[String]('l', "location")
        .action((x, c) => c.copy(location = x))
        .text("root directory of location to create data in")
      opt[String]('f', "format")
        .action((x, c) => c.copy(format = x))
        .text("valid spark format, Parquet, ORC ...")
      opt[Boolean]('i', "useDoubleForDecimal")
        .action((x, c) => c.copy(useDoubleForDecimal = x))
        .text("true to replace DecimalType with DoubleType")
      opt[Boolean]('e', "useStringForDate")
        .action((x, c) => c.copy(useStringForDate = x))
        .text("true to replace DateType with StringType")
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite the data that is already there")
      opt[Boolean]('p', "partitionTables")
        .action((x, c) => c.copy(partitionTables = x))
        .text("create the partitioned fact tables")
      opt[Boolean]('c', "clusterByPartitionColumns")
        .action((x, c) => c.copy(clusterByPartitionColumns = x))
        .text("shuffle to get partitions coalesced into single files")
      opt[Boolean]('v', "filterOutNullPartitionValues")
        .action((x, c) => c.copy(filterOutNullPartitionValues = x))
        .text("true to filter out the partition with NULL key value")
      opt[String]('t', "tableFilter")
        .action((x, c) => c.copy(tableFilter = x))
        .text("\"\" means generate all tables")
      opt[Int]('n', "numPartitions")
        .action((x, c) => c.copy(numPartitions = x))
        .text("how many dsdgen partitions to run - number of input tasks.")
      opt[String]('b', "database")
        .action((x, c) => c.copy(databaseName = x))
        .text("hive database \"\" means do not generate tables")
      opt[Boolean]('r', "dropExistingDatabase")
        .action((x, c) => c.copy(dropExistingDatabase = x))
        .text("true to drop existing database - default false")
      opt[Boolean]('z', "analyze")
        .action((x, c) => c.copy(analyzeTables = x))
        .text("true to analyze tables to use with CBO - default false")
      opt[Boolean]('y', "copytodatabse")
        .action((x, c) => c.copy(copyToDatabase = x))
        .text("copy tables from database to location")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, GenTPCDSDataConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  private def run(config: GenTPCDSDataConfig) {
    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    if (config.dsdgenDistArchive!="") {
      spark.sparkContext.addFile(config.dsdgenDistArchive)
    }


    val tables = new TPCDSTables(spark.sqlContext,
      dsdgenDir = config.dsdgenDir,
      scaleFactor = config.scaleFactor,
      useDoubleForDecimal = config.useDoubleForDecimal,
      useStringForDate = config.useStringForDate,
      dsdgenArchive = config.dsdgenDistArchive)

    val dsdgenNonPartitioned = 10 // small tables do not need much parallelism in generation.

    val nonPartitionedTables_raw = Array("call_center", "catalog_page", "customer", "customer_address", "customer_demographics", "date_dim", "household_demographics", "income_band", "item", "promotion", "reason", "ship_mode", "store",  "time_dim", "warehouse", "web_page", "web_site")
    val partitionedTables_raw = Array("inventory", "web_returns", "catalog_returns", "store_returns", "web_sales", "catalog_sales", "store_sales")

    val nonPartitionedTables =
    if (config.tableFilter != ""){
          nonPartitionedTables_raw.filter(_ == config.tableFilter)
      }
      else{
          nonPartitionedTables_raw
      }

    val partitionedTables =
      if (config.tableFilter != ""){
        partitionedTables_raw.filter(_ == config.tableFilter)
      }
      else{
        partitionedTables_raw
      }

    log.info("generate unpartitioned tables-->")

    if (config.databaseName != "") {
      log.info(s"create database ${config.databaseName}->")
      println(s"create database ${config.databaseName}->")
      if (config.dropExistingDatabase) {
        log.info(s"database ${config.databaseName} will be dropped")
        println(s"database ${config.databaseName} will be dropped")
        spark.sql(s"drop database if exists ${config.databaseName} cascade")
      }
      spark.sql(s"create database if not exists ${config.databaseName}")
      log.info(s"database ${config.databaseName} recreated")
      println(s"database ${config.databaseName} recreated")
      spark.sql(s"use ${config.databaseName}")
    }

    nonPartitionedTables.foreach { t => {
      tables.genData(
        location = config.location,
        format = config.format,
        overwrite = config.overwrite,
        partitionTables = config.partitionTables,
        clusterByPartitionColumns = config.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = dsdgenNonPartitioned,
        databaseName = config.databaseName,
        copyToDatabase = config.copyToDatabase
      )
      log.info(s"$t generated")
    }}
    log.info("<-- nonpartitioned tables done")

    log.info("generate partitioned tables-->")
    partitionedTables.foreach { t => {
      tables.genData(
        location = config.location,
        format = config.format,
        overwrite = config.overwrite,
        partitionTables = config.partitionTables,
        clusterByPartitionColumns = config.clusterByPartitionColumns,
        filterOutNullPartitionValues = config.filterOutNullPartitionValues,
        tableFilter = t,
        numPartitions = config.numPartitions,
        databaseName = config.databaseName,
        copyToDatabase = config.copyToDatabase)
      log.info(s"$t generated")
    }}
    log.info("<-- partitioned tables done")
    /*
      tables.genData(
      location = config.location,
      format = config.format,
      overwrite = config.overwrite,
      partitionTables = config.partitionTables,
      clusterByPartitionColumns = config.clusterByPartitionColumns,
      filterOutNullPartitionValues = config.filterOutNullPartitionValues,
      tableFilter = config.tableFilter,
      numPartitions = config.numPartitions)
     */
    if (config.databaseName!=""){
      if (!config.copyToDatabase) {
        log.info(s"create tables-->")
        println(s"create external tables-->")
        tables.createExternalTables(config.location, config.format, config.databaseName, overwrite = true, discoverPartitions = true)
        println(s"<--done")
        log.info(s"<--done ")
      }
      if (config.analyzeTables) {
        log.info(s"analyze tables-->")
        tables.analyzeTables(config.databaseName, analyzeColumns = true,tableFilter =  config.tableFilter)
        log.info(s"<--done ")
      }
    }
    else{
      log.info("database wil not be generated (by option)")
    }
  }
}
