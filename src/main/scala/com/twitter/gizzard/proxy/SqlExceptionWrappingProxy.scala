package com.twitter.gizzard
package proxy

import java.sql.SQLException
import shards.ShardId
import scala.reflect.Manifest
import com.mysql.jdbc.exceptions.MySQLTransientException
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorProxy}

class SqlExceptionWrappingProxy(shardId: ShardId) extends ExceptionHandlingProxy({e =>
  e match {
    case e: SqlQueryTimeoutException =>
      throw new shards.ShardTimeoutException(e.timeout, shardId, e)
    case e: SqlDatabaseTimeoutException =>
      throw new shards.ShardDatabaseTimeoutException(e.timeout, shardId, e)
    case e: MySQLTransientException =>
      throw new shards.NormalShardException(e.toString, shardId, null)
    case e: SQLException =>
      if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
        throw new shards.NormalShardException(e.toString, shardId, null)
      } else {
        throw new shards.ShardException(e.toString, e)
      }
    case e: shards.ShardException =>
      throw e
  }
})

class SqlExceptionWrappingProxyFactory[T <: AnyRef : Manifest](id: ShardId) extends ExceptionHandlingProxyFactory[T]({ (shard, e) =>
  val manifest = implicitly[Manifest[T]]

  e match {
    case e: SqlQueryTimeoutException =>
      throw new shards.ShardTimeoutException(e.timeout, id, e)
    case e: SqlDatabaseTimeoutException =>
      throw new shards.ShardDatabaseTimeoutException(e.timeout, id, e)
    case e: MySQLTransientException =>
      throw new shards.NormalShardException(e.toString, id, null)
    case e: SQLException =>
      if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
        throw new shards.NormalShardException(e.toString, id, null)
      } else {
        throw new shards.ShardException(e.toString, e)
      }
    case e: shards.ShardException =>
      throw e
  }
})

class ShardExceptionWrappingQueryEvaluator(shardId: ShardId, evaluator: QueryEvaluator) extends QueryEvaluatorProxy(evaluator) {
  override protected def delegate[A](f: => A) = {
    try {
      f
    } catch {
      case e: SqlQueryTimeoutException =>
        throw new shards.ShardTimeoutException(e.timeout, shardId, e)
      case e: SqlDatabaseTimeoutException =>
        throw new shards.ShardDatabaseTimeoutException(e.timeout, shardId, e)
      case e: MySQLTransientException =>
        throw new shards.NormalShardException(e.toString, shardId, null)
      case e: SQLException =>
        if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
          throw new shards.NormalShardException(e.toString, shardId, null)
        } else {
          throw new shards.ShardException(e.toString, e)
        }
      case e: shards.ShardException =>
        throw e
     }
  }
}
