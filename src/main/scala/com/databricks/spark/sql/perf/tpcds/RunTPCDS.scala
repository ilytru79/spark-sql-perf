package com.databricks.spark.sql.perf.tpcds

import com.databricks.spark.sql.perf.tpcds.GenTPCDSData.getClass
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.spark.sql.functions.{col, lit, substring}

case class RunTPCDSConfig(     master: String = "local[*]",
                               scaleFactor: String = null,
                               resultLocation: String = "",
                               useDoubleForDecimal: Boolean = false,
                               useStringForDate: Boolean = false,
                               queryFilter: String = "",
                               numIterations: Int = 2,
                               databaseName: String = "",
                               timeoutHours: Int = 60,
                               randomizeQueries: Boolean = false)

object RunTPCDS {
  private val log = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunTPCDSConfig]("Run-TPC-DS") {
      opt[String]('m', "master")
        .action { (x, c) => c.copy(master = x) }
        .text("the Spark master to use, default to local[*]")
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
      opt[String]('l', "resultLocation")
        .action((x, c) => c.copy(resultLocation = x))
        .text("root directory of location to place results")
      opt[Boolean]('i', "useDoubleForDecimal")
        .action((x, c) => c.copy(useDoubleForDecimal = x))
        .text("true to replace DecimalType with DoubleType")
      opt[Boolean]('e', "useStringForDate")
        .action((x, c) => c.copy(useStringForDate = x))
        .text("true to replace DateType with StringType")
      opt[String]('t', "queryFilter")
        .action((x, c) => c.copy(queryFilter = x))
        .text("\"\" means run all queries")
      opt[Int]('n', "numIterations")
        .action((x, c) => c.copy(numIterations = x))
        .text("how many times to run the test")
      opt[String]('b', "database")
        .action((x, c) => c.copy(databaseName = x))
        .text("hive database \"\" means do not generate tables")
      opt[Int]('w', "timeoutHours")
        .action((x, c) => c.copy(timeoutHours = x))
        .text("how many to wait test execution before fail")
      opt[Boolean]('r', "randomizeQueries")
        .action((x, c) => c.copy(randomizeQueries = x))
        .text("true to analyze tables to use with CBO - default false")
    }

    parser.parse(args, RunTPCDSConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }
  private def run(config: RunTPCDSConfig): Unit = {
    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()
    val queryFilter = if (config.queryFilter==""){
      Seq()
    } else {
      config.queryFilter.split(",").toSeq
    }
    spark.sql(s"use ${config.databaseName}")
    val tpcds = new TPCDS (sqlContext = spark.sqlContext)
    def queries = {
      val filtered_queries = queryFilter match {
        case Seq() => tpcds.tpcds2_4Queries
        case _ => tpcds.tpcds2_4Queries.filter(q => queryFilter.contains(q.name))
      }
      if (config.randomizeQueries) scala.util.Random.shuffle(filtered_queries) else filtered_queries
    }
    val experiment = tpcds.runExperiment(
      queries,
      iterations = config.numIterations,
      resultLocation = config.resultLocation,
      tags = Map("runtype" -> "benchmark", "database" -> config.databaseName, "scale_factor" -> config.scaleFactor))

    println(experiment.toString)
    experiment.waitForFinish(config.timeoutHours*60*60)


    val summary = experiment.getCurrentResults
      .withColumn("Name", substring(col("name"), 2, 100))
      .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)

    summary.write.mode("overwrite").json(s"${config.resultLocation}/totalResults.json")
    println (s"Experiment done reuslts are in ${config.resultLocation}")
  }
}
