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

/**
 * A factory for type-bound exception-handling proxies. It creates proxies for
 * a specific class. It has much better runtime performance (about 30x on 
 * current hardware and JVM) when creating proxies than ExceptionHandlingProxy
 * does, as it memoizes the expensive class-specific lookup.
 * @tparam T the type of the objects that will be proxied
 * @param f the exception handling function
 * @author Attila Szegedi
 */
class ExceptionHandlingProxyFactory[T <: AnyRef](f: Throwable => Unit)(implicit manifest: Manifest[T]) {
  val proxyFactory = new ProxyFactory[T]
  /**
   * Creates an exception-handling proxy for the specific object where each 
   * method call on the proxy is wrapped in an exception handler.
   * @param obj the object being proxied
   * @tparam I the exposed interface of the created proxy. Must be an 
   * interface the object implements.
   * @return a proxy for the object, implementing the requested interface, 
   * that has each of its method invocations wrapped in an exception handler.
   */
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
