package com.rikima.cli

import com.spotify.scio._

//case class Record(c1: Int, c2: String)

object Leads {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(Array(
      //"--runner=DirectRunner",
      "--runner=DataflowRunner",
      "--region=asia-northeast2",
      "--zone=asia-northeast2-a",
      "--numWorkers=5",
      "--maxNumberWorkers=10",
      "--directRunnerParallelism=5",
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
        val c1 = csv(0)
        val c2 = csv(1)
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
