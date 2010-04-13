package com.twitter.gizzard.test

import com.twitter.querulous.database.ApachePoolingDatabaseFactory
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.Configgy


trait Database {
  val config = Configgy.config
  val databaseName = config("db.database")
  val databaseHostname = config("db.hostname")
  val databaseUsername = config("db.username")
  val databasePassword = config("db.password")

  val databaseFactory = new ApachePoolingDatabaseFactory(
    config("db.connection_pool.size_min").toInt,
    config("db.connection_pool.size_max").toInt,
    config("db.connection_pool.test_idle_msec").toLong.millis,
    config("db.connection_pool.max_wait").toLong.millis,
    config("db.connection_pool.test_on_borrow").toBoolean,
    config("db.connection_pool.min_evictable_idle_msec").toLong.millis)

  val queryEvaluatorFactory = try {
    val sqlQueryFactory = new SqlQueryFactory
    new StandardQueryEvaluatorFactory(databaseFactory, sqlQueryFactory)
  } catch {
    case e =>
      println(e.toString())
      throw e
  }

  def getQueryEvaluator() = try {
    val topLevelEvaluator = queryEvaluatorFactory(databaseHostname, null, databaseUsername, databasePassword)
    topLevelEvaluator.execute("DROP DATABASE IF EXISTS " + databaseName)
    topLevelEvaluator.execute("CREATE DATABASE " + databaseName)
    queryEvaluatorFactory(databaseHostname, databaseName, databaseUsername, databasePassword)
  } catch {
    case e =>
      println(e.toString())
      throw e
  }
}
