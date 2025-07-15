package com.rikima.cli

import com.spotify.scio._
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(//cmdlineArgs)
      Array(
        "--runner=DirectRunner",
        "--tempLocation=gs://rikitoku-sandbox/tmp",
      ))

    val exampleData = "gs://dataflow-samples/shakespeare/kinglear.txt"
    val input = args.getOrElse("input", exampleData)
    val output = args.getOrElse("output", "gs://rikitoku-sandbox/wc")

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
