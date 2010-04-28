package com.twitter.gizzard.test

import com.twitter.querulous.database.{SingleConnectionDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{ConfigMap, Configgy}


trait Database {
  val poolConfig: ConfigMap

  lazy val databaseFactory = try {
    new MemoizingDatabaseFactory(new SingleConnectionDatabaseFactory)
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }

  def queryEvaluatorFactory = try {
    val sqlQueryFactory = new SqlQueryFactory
    new StandardQueryEvaluatorFactory(databaseFactory, sqlQueryFactory)
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }

  def evaluator(configMap: ConfigMap) = try {
    queryEvaluatorFactory(configMap("hostname"), configMap("database"), configMap("username"), configMap("password"))
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }

  def rootEvaluator(configMap: ConfigMap) = try {
    queryEvaluatorFactory(configMap("hostname"), null, configMap("username"), configMap("password"))
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }
}

