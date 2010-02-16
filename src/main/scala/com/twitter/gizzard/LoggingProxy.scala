package com.twitter.gizzard

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy => JProxy, UndeclaredThrowableException}
import scala.reflect.Manifest
import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a W3CStats logger.
 */
object LoggingProxy {
  var counter = 0

  def apply[T <: AnyRef](logger: W3CStats, name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { (unused, method, args) =>
      val shortName = name.lastIndexOf('.') match {
        case -1 => name
        case n => name.substring(n + 1)
      }

      logger.transaction {
        logger.log("operation", shortName + ":" + method.name)
        val arguments = (if (args != null) args.mkString(",") else "").replace(' ', '_')
        logger.log("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
        logger.time("action-timing") {
          method.invoke(obj, args)
        }
      }
    }
  }
}

object Proxy {
  def apply[T <: AnyRef](obj: T)(f: (T, MethodWrapper, Array[Object]) => Object)(implicit manifest: Manifest[T]): T = {
    val invocationHandler = new InvocationHandler {
      def invoke(unused: Object, method: Method, args: Array[Object]) = {
        f(obj, new MethodWrapper(method), args)
      }
    }

    JProxy.newProxyInstance(obj.getClass.getClassLoader, Array(manifest.erasure), invocationHandler).asInstanceOf[T]
  }

  def apply[T <: AnyRef](cls: Class[T])(f: (T, MethodWrapper, Array[Object]) => Object)(implicit manifest: Manifest[T]): T = {
    val invocationHandler = new InvocationHandler {
      def invoke(obj: Object, method: Method, args: Array[Object]) = {
        f(obj.asInstanceOf[T], new MethodWrapper(method), args)
      }
    }

    JProxy.newProxyInstance(cls.getClassLoader, Array(manifest.erasure), invocationHandler).asInstanceOf[T]
  }

  class MethodWrapper(val method: Method) {
    @throws(classOf[Exception])
    def invoke(obj: Object, args: Array[Object]) = {
      try {
        method.invoke(obj, args: _*)
      } catch {
        case e: InvocationTargetException => throw e.getTargetException
      }
    }

    def name = method.getName
  }
}
