package com.twitter.gizzard.proxy

import java.sql.SQLException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException


object SqlExceptionWrappingProxy extends ExceptionHandlingProxy({ e =>
  e match {
    case e: SqlQueryTimeoutException =>
      throw new shards.ShardTimeoutException(e.timeout, e)
    case e: SqlDatabaseTimeoutException =>
      throw new shards.ShardDatabaseTimeoutException(e.timeout, e)
    case e: SQLException =>
      throw new shards.ShardException(e.toString, e)
    case e: shards.ShardException =>
      throw e
  }
})
