package com.twitter.gizzard.test

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.{ConfigMap, Configgy}
import org.specs.Specification


trait IdServerDatabase extends Specification with Database {
  def reset(config: ConfigMap) {
    config.getConfigMap("ids").foreach { id =>
      val idQueryEvaluator = evaluator(id)
      idQueryEvaluator.execute("DELETE FROM " + id("table"))
      idQueryEvaluator.execute("INSERT INTO " + id("table") + " VALUES (0)")
    }
  }

  def materialize(config: ConfigMap) {
    config.getConfigMap("ids").foreach { id =>
      val queryEvaluator = rootEvaluator(id)
      queryEvaluator.execute("CREATE DATABASE IF NOT EXISTS " + id("database"))
    }
  }
}

trait NameServerDatabase extends Specification with Database {
  def materialize(config: ConfigMap) {
    val key = config.configMap("nameservers").keys.next
    val ns = config.configMap("nameservers." + key)
    val evaluator = rootEvaluator(ns)
    evaluator.execute("CREATE DATABASE IF NOT EXISTS " + ns("database"))
  }

  def reset(config: ConfigMap) {
    val key = config.configMap("nameservers").keys.next
    val ns = config.configMap("nameservers." + key)
    val nameServerQueryEvaluator = evaluator(ns)
    reset(nameServerQueryEvaluator)
  }

  def reset(queryEvaluator: QueryEvaluator) {
    queryEvaluator.execute("DELETE FROM forwardings")
    queryEvaluator.execute("DELETE FROM shard_children")
    queryEvaluator.execute("DELETE FROM shards")
  }

/*  def reset(database: String) {
    Time.reset()
    Time.freeze()
    val queryEvaluator = queryEvaluatorFactory("localhost", null, config("db.username"), config("db.password"))
    queryEvaluator.execute("DROP DATABASE IF EXISTS " + database)
  } */
}
