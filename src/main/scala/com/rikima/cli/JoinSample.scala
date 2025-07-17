package com.rikima.cli

import com.spotify.scio._

object JoinSample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(Array(
      //"--runner=DirectRunner",
      "--runner=DataflowRunner",
      "--region=asia-northeast2",
      "--zone=asia-northeast2-b",
      "--numWorkers=2",
      "--maxNumberWorkers=2",
      "--directRunnerParallelism=2",
      //"--worker-machine-type=e2-standard-2",
      //"--output=./wc"
    )
    )

    println(sc.options)

    //val input = args.getOrElse("input", "gs://rikitoku-sandbox/input/test.csv")
    val leadsCsv     = "gs://rikitoku-sandbox/input/leads/leads-10000000.csv" // args.getOrElse("input", "gs://rikitoku-sandbox/input/leads/*.csv")
    val customersCsv = "gs://rikitoku-sandbox/input/customers/customers-100000.csv"
    
    val output = args.getOrElse("output", "gs://rikitoku-sandbox/output/joined")

    val leads = sc.textFile(leadsCsv).map(_.trim)
      //.map(row => row.replace(""""""", ""))
      //.filter(row => row.split(",")(1).matches("\\d+"))
      .map { row =>
        val csv = row.split(",")
        val c1 = csv(2)
        val c2 = csv(3)
        (c1, c2)
      }
      .map { t =>
        (t._1, t)
      }
      .groupByKey.mapValues(_.toSeq)
      

    val customers = sc.textFile(customersCsv).map(_.trim)
      //.map(row => row.replace(""""""", ""))
      //.filter(row => row.split(",")(1).matches("\\d+"))
      .map { row =>
        val csv = row.split(",")
        val c1 = csv(2)
        val c2 = csv(3)
        (c1, c2)
      }
      .map { t =>
        (t._1, t)
      }
      .groupByKey.mapValues(_.toSeq)

    //rows.take(10).debug(prefix="rows:")
    //rows.count.debug(prefix="#of rows:")
    
    val joined = leads.join(customers)
    joined.saveAsTextFile(output)

    sc.run()
    // val result = sc.run().waitUntilDone()
  }
}
