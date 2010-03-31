package com.twitter.gizzard.proxy

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException


object SqlExceptionWrappingProxy {
  def apply[T <: AnyRef](obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      try {
        method()
      } catch {
        case ex: Throwable =>
          (ex match {
            case ex: UndeclaredThrowableException => ex.getCause()
            case ex => ex
          }) match {
            case e: SqlQueryTimeoutException =>
              throw new shards.ShardTimeoutException
            case e: SqlDatabaseTimeoutException =>
              throw new shards.ShardDatabaseTimeoutException
            case e: SQLException =>
              throw new shards.ShardException(e.toString)
          }
      }
    }
  }
}
