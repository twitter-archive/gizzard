package com.twitter.gizzard.test

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import net.lag.configgy.{ConfigMap, Configgy}
import org.specs.Specification


trait IdServerDatabase extends Specification with Database {
  def materialize(config: ConfigMap) {
    try {
      config.getConfigMap("ids").foreach { id =>
        val queryEvaluator = rootEvaluator(id)
        queryEvaluator.execute("DROP DATABASE IF EXISTS " + id("database"))
        queryEvaluator.execute("CREATE DATABASE " + id("database"))
      }
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(config: ConfigMap) {
    try {
      config.getConfigMap("ids").foreach { id =>
        val idQueryEvaluator = evaluator(id)
        idQueryEvaluator.execute("DELETE FROM " + id("table"))
        idQueryEvaluator.execute("INSERT INTO " + id("table") + " VALUES (0)")
      }
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }
}

trait NameServerDatabase extends Specification with Database {
  def materialize(config: ConfigMap) {
    try {
      val ns = config.getConfigMap("replicas").map { map =>
        val key = map.keys.next
        config.configMap("replicas." + key)
      } getOrElse(config)
      val evaluator = rootEvaluator(ns)
      evaluator.execute("DROP DATABASE IF EXISTS " + ns("database"))
      evaluator.execute("CREATE DATABASE " + ns("database"))
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(config: ConfigMap) {
    try {
      val ns = config.getConfigMap("replicas").map { map =>
        val key = map.keys.next
        config.configMap("replicas." + key)
      } getOrElse(config)
      val nameServerQueryEvaluator = evaluator(ns)
      reset(nameServerQueryEvaluator)
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(queryEvaluator: QueryEvaluator) {
    List("forwardings", "shard_children", "shards").foreach { tableName =>
      try {
        queryEvaluator.execute("DELETE FROM " + tableName)
      } catch {
        case e =>
          // it's okay. might not be such a table yet!
          try {
            queryEvaluator.execute("DROP TABLE IF EXISTS " + tableName)
          } catch {
            case e =>
              e.printStackTrace()
              throw e
          }
      }
    }
  }
}
