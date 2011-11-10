package com.twitter.gizzard.proxy

import java.sql.SQLException
import scala.reflect.Manifest
import com.mysql.jdbc.MysqlErrorNumbers
import com.mysql.jdbc.exceptions.MySQLTransientException
import com.twitter.querulous.database.{PoolTimeoutException, PoolEmptyException, SqlDatabaseTimeoutException}
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorProxy}
import com.twitter.gizzard.shards._


class SqlExceptionWrappingProxy(shardId: ShardId) extends ExceptionHandlingProxy({e =>
  e match {
    case e: SqlQueryTimeoutException =>
      throw new ShardTimeoutException(e.timeout, shardId, e)
    case e: SqlDatabaseTimeoutException =>
      throw new ShardDatabaseTimeoutException(e.timeout, shardId, e)
    case e: PoolTimeoutException =>
      throw new NormalShardException(e.toString, shardId, e)
    case e: PoolEmptyException =>
      throw new NormalShardException(e.toString, shardId, e)
    case e: MySQLTransientException =>
      throw new NormalShardException(e.toString, shardId, null)
    case e: SQLException =>
      if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
        throw new NormalShardException(e.toString, shardId, null)
      } else if (e.getErrorCode() == MysqlErrorNumbers.ER_OPTION_PREVENTS_STATEMENT) {
        throw new ShardOfflineException(shardId)
      } else {
        throw new ShardException(e.toString, e)
      }
    case e: ShardException =>
      throw e
  }
})

class SqlExceptionWrappingProxyFactory[T <: AnyRef : Manifest](id: ShardId) extends ExceptionHandlingProxyFactory[T]({ (shard, e) =>
  val manifest = implicitly[Manifest[T]]

  e match {
    case e: SqlQueryTimeoutException =>
      throw new ShardTimeoutException(e.timeout, id, e)
    case e: SqlDatabaseTimeoutException =>
      throw new ShardDatabaseTimeoutException(e.timeout, id, e)
    case e: PoolTimeoutException =>
      throw new NormalShardException(e.toString, id, e)
    case e: PoolEmptyException =>
      throw new NormalShardException(e.toString, id, e)
    case e: MySQLTransientException =>
      throw new NormalShardException(e.toString, id, null)
    case e: SQLException =>
      if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
        throw new NormalShardException(e.toString, id, null)
      } else if (e.getErrorCode() == MysqlErrorNumbers.ER_OPTION_PREVENTS_STATEMENT) {
        throw new ShardOfflineException(id)
      } else {
        throw new ShardException(e.toString, e)
      }
    case e: ShardException =>
      throw e
  }
})

class ShardExceptionWrappingQueryEvaluator(shardId: ShardId, evaluator: QueryEvaluator) extends QueryEvaluatorProxy(evaluator) {
  override protected def delegate[A](f: => A) = {
    try {
      f
    } catch {
      case e: SqlQueryTimeoutException =>
        throw new ShardTimeoutException(e.timeout, shardId, e)
      case e: SqlDatabaseTimeoutException =>
        throw new ShardDatabaseTimeoutException(e.timeout, shardId, e)
      case e: PoolTimeoutException =>
        throw new NormalShardException(e.toString, shardId, e)
      case e: PoolEmptyException =>
        throw new NormalShardException(e.toString, shardId, e)
      case e: MySQLTransientException =>
        throw new NormalShardException(e.toString, shardId, null)
      case e: SQLException =>
        if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
          throw new NormalShardException(e.toString, shardId, null)
        } else if (e.getErrorCode() == MysqlErrorNumbers.ER_OPTION_PREVENTS_STATEMENT) {
          throw new ShardOfflineException(shardId)
        } else {
          throw new ShardException(e.toString, e)
        }
      case e: ShardException =>
        throw e
     }
  }
}
