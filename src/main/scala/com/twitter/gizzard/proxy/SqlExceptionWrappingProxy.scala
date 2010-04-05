package com.twitter.gizzard.proxy

import java.sql.SQLException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException


object SqlExceptionWrappingProxy extends ExceptionHandlingProxy({e =>
  e match {
    case e: SqlQueryTimeoutException =>
      throw new shards.ShardTimeoutException
    case e: SqlDatabaseTimeoutException =>
      throw new shards.ShardDatabaseTimeoutException
    case e: SQLException =>
      throw new shards.ShardException(e.toString)
  }
})
