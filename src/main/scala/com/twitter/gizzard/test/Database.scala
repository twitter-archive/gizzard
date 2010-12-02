package com.twitter.gizzard.test

import com.twitter.querulous.database.{SingleConnectionDatabaseFactory, MemoizingDatabaseFactory}
import com.twitter.querulous.evaluator.{QueryEvaluator, StandardQueryEvaluatorFactory}
import com.twitter.querulous.config.Connection
import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.util.TimeConversions._



trait Database {
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

  def evaluator(connection: Connection) = try {
    queryEvaluatorFactory(connection)
  } catch {
    case e =>
      e.printStackTrace()
      throw e
  }

  def rootEvaluator(config: Connection) = evaluator(config.withoutDatabase)
}

