package com.twitter.gizzard.test

import com.twitter.querulous.database.{ApachePoolingDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{ConfigMap, Configgy}


trait Database {
  val poolConfig: ConfigMap

  val databaseFactory = new MemoizingDatabaseFactory(new ApachePoolingDatabaseFactory(
    poolConfig("size_min").toInt,
    poolConfig("size_max").toInt,
    poolConfig("test_idle_msec").toLong.millis,
    poolConfig("max_wait").toLong.millis,
    poolConfig("test_on_borrow").toBoolean,
    poolConfig("min_evictable_idle_msec").toLong.millis))

  val queryEvaluatorFactory = try {
    val sqlQueryFactory = new SqlQueryFactory
    new StandardQueryEvaluatorFactory(databaseFactory, sqlQueryFactory)
  } catch {
    case e =>
      println(e.toString())
      throw e
  }

  def evaluator(configMap: ConfigMap) = queryEvaluatorFactory(configMap("hostname"), configMap("database"), configMap("username"), configMap("password"))
  def rootEvaluator(configMap: ConfigMap) = queryEvaluatorFactory(configMap("hostname"), null, configMap("username"), configMap("password"))
}

