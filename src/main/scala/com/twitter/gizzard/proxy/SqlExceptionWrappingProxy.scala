package com.twitter.gizzard.proxy

import java.sql.SQLException
import shards.ShardId
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorProxy}


class SqlExceptionWrappingProxy(shardId: ShardId) extends ExceptionHandlingProxy({e =>
  e match {
    case e: SqlQueryTimeoutException =>
      throw new shards.ShardTimeoutException(shardId, e)
    case e: SqlDatabaseTimeoutException =>
      throw new shards.ShardDatabaseTimeoutException(shardId, e)
    case e: SQLException =>
      throw new shards.ShardException(e.toString, e)
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
        throw new shards.ShardTimeoutException(shardId, e)
      case e: SqlDatabaseTimeoutException =>
        throw new shards.ShardDatabaseTimeoutException(shardId, e)
      case e: SQLException =>
        throw new shards.ShardException(e.toString, e)
      case e: shards.ShardException =>
        throw e
     }
  }
}
