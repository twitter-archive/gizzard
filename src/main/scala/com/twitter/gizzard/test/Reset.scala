package com.twitter.gizzard
package test

import com.twitter.querulous.query.SqlQueryFactory
import com.twitter.querulous.evaluator.{StandardQueryEvaluatorFactory, QueryEvaluator}
import com.twitter.util.Time
import com.twitter.util.TimeConversions._
import org.specs.Specification


// trait IdServerDatabase extends Specification with Database {
//   def materialize(config: ConfigMap) {
//     try {
//       config.getConfigMap("ids").foreach { id =>
//         val queryEvaluator = rootEvaluator(id)
//         queryEvaluator.execute("DROP DATABASE IF EXISTS " + id("database"))
//         queryEvaluator.execute("CREATE DATABASE " + id("database"))
//       }
//     } catch {
//       case e =>
//         e.printStackTrace()
//         throw e
//     }
//   }
//
//   def reset(config: ConfigMap) {
//     try {
//       config.getConfigMap("ids").foreach { id =>
//         val idQueryEvaluator = evaluator(id)
//         idQueryEvaluator.execute("DELETE FROM " + id("table"))
//         idQueryEvaluator.execute("INSERT INTO " + id("table") + " VALUES (0)")
//       }
//     } catch {
//       case e =>
//         e.printStackTrace()
//         throw e
//     }
//   }
// }

trait NameServerDatabase extends Specification {
  def materialize(cfg: config.NameServer) {
    try {
      cfg.replicas.map {
        case mysql: config.Mysql => {
          val conn = mysql.connection
          val evaluator = mysql.queryEvaluator()(conn.withoutDatabase)
          evaluator.execute("DROP DATABASE IF EXISTS " + conn.database)
          evaluator.execute("CREATE DATABASE " + conn.database)
        }
        case _ => ()
      }
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def evaluator(cfg: config.NameServer): QueryEvaluator = {
    cfg.replicas.flatMap({
      case m: config.Mysql => Seq(m.queryEvaluator()(m.connection))
      case _ => Nil
    }).head
  }

  def reset(cfg: config.NameServer) {
    try {
      reset(evaluator(cfg))
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    }
  }

  def reset(queryEvaluator: QueryEvaluator) {
    List("forwardings", "shard_children", "shards", "hosts").foreach { tableName =>
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
