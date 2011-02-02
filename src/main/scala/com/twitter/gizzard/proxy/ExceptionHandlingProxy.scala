package com.twitter.gizzard.proxy

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.concurrent.ExecutionException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException


class ExceptionHandlingProxy[T <: AnyRef](f: Throwable => Unit)(implicit manifest: Manifest[T]) {
  val proxyFactory = new ProxyFactory[T]
  def apply(obj: T): T = {
    proxyFactory(obj) { method =>
      try {
        method()
      } catch {
        case ex: UndeclaredThrowableException => f(ex.getCause())
        case ex: ExecutionException => f(ex.getCause())
        case ex => f(ex)
      }
    }
  }
}
