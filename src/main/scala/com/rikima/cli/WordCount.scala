package com.rikima.cli

import com.spotify.scio._
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(Array(
      "--runner=DirectRunner",
      "--gcpTempLocation=gs://rikitoku-sandbox/tmp",
      "--stagingLocation=gs://rikitoku-sandbox/staging"
    ))

    /*
    sc.optionsAs[DataflowPipelineOptions]
      .setGcpTempLocation("gs://rikitoku-sandbox/tmp")

    sc.optionsAs[DataflowPipelineOptions]
      .setStagingLocation("gs://rikitoku-sandbox/staging")
     */

    val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
    val input  = exampleData // args.getOrElse("input", exampleData)
    val output = "gs://rikitoku-sandbox/wc" //args.getOrElse("output", "gs://rikitoku-sandbox/wc")

    println(s"input ${input}")
    println(s"output: ${output}")

    // ResourceIdを作成
    val src: ResourceId = FileSystems.matchNewResource(input, false)
    val dest: ResourceId = FileSystems.matchNewResource(output, false)

    // スキームとパスをログ出力
    println(s"srcResourceId: $src, Scheme: ${src.getScheme}")
    println(s"destResourceId: $dest, Scheme: ${dest.getScheme}")

    sc.textFile(input)
      .map(_.trim)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    sc.run()
    //val result = sc.run().waitUntilDone()
  }
}
