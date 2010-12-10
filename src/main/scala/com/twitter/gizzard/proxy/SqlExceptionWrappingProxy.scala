/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.gizzard.proxy

import java.sql.{SQLException, SQLTransactionRollbackException}
import shards.ShardId
import scala.reflect.Manifest
import com.mysql.jdbc.exceptions.MySQLTransientException
import com.mysql.jdbc.exceptions.jdbc4.{MySQLTransientException => MySQLTransientException4}
import com.twitter.ostrich.Stats
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException
import com.twitter.querulous.evaluator.{QueryEvaluator, QueryEvaluatorProxy}

object UnwrapException {
  def apply(exception: Throwable, shardId: ShardId) = {
    exception match {
      case e: SqlQueryTimeoutException =>
        new shards.ShardTimeoutException(e.timeout, shardId, e)
      case e: SqlDatabaseTimeoutException =>
        new shards.ShardDatabaseTimeoutException(e.timeout, shardId, e)
      case e: MySQLTransientException =>
        new shards.NormalShardException(e.toString, shardId, null)
      case e: MySQLTransientException4 =>
        new shards.NormalShardException(e.toString, shardId, null)
      case e: SQLTransactionRollbackException =>
        new shards.NormalShardException(e.toString, shardId, null)
      case e: SQLException =>
        if ((e.toString contains "Connection") && (e.toString contains " is closed")) {
          new shards.NormalShardException(e.toString, shardId, null)
        } else {
          new shards.ShardException(e.toString, e)
        }
      case e: shards.ShardException =>
        e
    }
  }
}

/**
 * Helper class that catches common SQL exceptions and translates them into gizzard shard
 * exceptions. Not used directly in gizzard itself, but provided as a convenience. FlockDB
 * has an example use in SqlShard.
 */
class SqlExceptionWrappingProxy(shardId: ShardId) extends ExceptionHandlingProxy({ e =>
  if (shardId.hostname != "localhost") Stats.incr("db_exceptions_" + shardId.hostname)
  throw UnwrapException(e, shardId)
})

class ShardExceptionWrappingQueryEvaluator(shardId: ShardId, evaluator: QueryEvaluator) extends QueryEvaluatorProxy(evaluator) {
  override protected def delegate[A](f: => A) = {
    try {
      f
    } catch {
      case e: Throwable =>
        if (shardId.hostname != "localhost") Stats.incr("db_exceptions_" + shardId.hostname)
        throw UnwrapException(e, shardId)
    }
  }
}
