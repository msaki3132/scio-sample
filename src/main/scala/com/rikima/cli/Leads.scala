package com.rikima.cli

import com.spotify.scio._

object Leads {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(Array(
      //"--runner=DirectRunner",
      "--runner=DataflowRunner",
      "--region=asia-northeast2",
      // "--zone=asia-northeast1-a",
      "--numWorkers=10",
      "--maxNumberWorkers=20",
      "--directRunnerParallelism=20",
      "--worker-machine-type=e2-standard-2",
      //"--output=./wc"
    )
    )

    println(sc.options)

    //val input = args.getOrElse("input", "gs://rikitoku-sandbox/input/test.csv")
    val input = args.getOrElse("input", "gs://rikitoku-sandbox/input/leads/leads-10000000.csv")
    val output = args.getOrElse("output", "gs://rikitoku-sandbox/output/leads")

    val rows = sc.textFile(input).map(_.trim)
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
    rows.saveAsTextFile(output)

    sc.run()
    // val result = sc.run().waitUntilDone()
  }
}
