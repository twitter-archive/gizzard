package com.twitter.gizzard.proxy

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.concurrent.ExecutionException
import scala.reflect.Manifest
import com.twitter.querulous.database.SqlDatabaseTimeoutException
import com.twitter.querulous.query.SqlQueryTimeoutException


class ExceptionHandlingProxy(f: Throwable => Unit) {
  def apply[T <: AnyRef](obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
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

class ExceptionHandlingProxyFactory[T <: AnyRef](f: Throwable => Unit)(implicit manifest: Manifest[T]) {
  val proxyFactory = new ProxyFactory[T]
  def apply[I >: T](obj: T): I = {
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