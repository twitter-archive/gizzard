package com.twitter.gizzard.proxy

import java.sql.SQLException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException
import java.lang.reflect.UndeclaredThrowableException


class ExceptionHandlingProxy(f: Throwable => Unit) {
  def apply[T <: AnyRef](obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      try {
        method()
      } catch {
        case ex: UndeclaredThrowableException => f(ex.getCause())
        case ex => f(ex)
      }
    }
  }
}
