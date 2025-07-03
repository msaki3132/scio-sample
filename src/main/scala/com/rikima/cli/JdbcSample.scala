package com.rikima.cli

import com.spotify.scio._
import com.spotify.scio.jdbc.{JdbcConnectionOptions, jdbcScioContextOps}

import java.sql.ResultSet

case class Record(c1: Int, c2: String)

object JdbcSample {
  val query = "select * from test limit 100000"

  val connectionUrl = "jdbc:mysql://localhost:3306/test"

  val connectionOptions = JdbcConnectionOptions(
    username = "root",
    password = Option("root"),
    connectionUrl = connectionUrl,
    classOf[com.mysql.cj.jdbc.Driver]
  )

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(Array("--numWorker=10", "--maxNumWorkers=50"))

    val rows = sc.jdbcSelect(connectionOptions,
      query,
      fetchSize=1000) { rs: ResultSet =>
      Record(
        c1=rs.getInt("c1"),
        c2=rs.getString("c2")
      )
    }

    rows.count.debug(prefix="#of rows:")
    rows.take(1).debug(prefix = "sampled rows:")
    //rows.debug(prefix="rows:")

    sc.run()
  }
}
