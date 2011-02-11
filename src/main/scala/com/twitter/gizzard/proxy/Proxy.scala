package com.twitter.gizzard.proxy

import java.lang.reflect
import scala.reflect.Manifest
import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger


/**
 * Helper for creating java proxy objects.
 *
 * Wraps an object or class into a java Proxy that implements the specified interface. When a
 * method is called on the proxy object, the given code block is called with a MethodCall object
 * that contains the details of the call to the underlying object. It can be called (via #apply)
 * to invoke the inner method call.
 */
object Proxy {
  def apply[T <: AnyRef](obj: T)(f: MethodCall[T] => Object): T = {
    val cls = obj.getClass
    val invocationHandler = new reflect.InvocationHandler {
      def invoke(unused: Object, method: reflect.Method, args: Array[Object]) = {
        f(new MethodCall(obj, method, args))
      }
    }

    reflect.Proxy.newProxyInstance(cls.getClassLoader, cls.getInterfaces,
                                   invocationHandler).asInstanceOf[T]
  }

  def apply[T <: AnyRef](cls: Class[T])(f: MethodCall[T] => Object): T = {
    val invocationHandler = new reflect.InvocationHandler {
      def invoke(obj: Object, method: reflect.Method, args: Array[Object]) = {
        f(new MethodCall(obj.asInstanceOf[T], method, args))
      }
    }

    reflect.Proxy.newProxyInstance(cls.getClassLoader, cls.getInterfaces,
                                   invocationHandler).asInstanceOf[T]
  }

  /**
   * A method call on a particular object with a particular set of arguments. When called,
   * exceptions are unwrapped from java's InvocationTargetException.
   */
  class MethodCall[T](var obj: T, var method: reflect.Method, var args: Array[Object]) {
    @throws(classOf[Exception])
    def apply() = {
      try {
        method.invoke(obj, args: _*)
      } catch {
        case e: reflect.InvocationTargetException =>
          throw e.getTargetException
      }
    }

    def name = method.getName
  }
}

/**
 * A factory for type-bound proxies. It creates proxies for a specific class.
 * It has much better runtime performance (about 30x on current hardware and
 * JVM) when creating proxies than Proxy does, as it memoizes the expensive
 * class-specific lookup.
 * @tparam T the type of the objects that will be proxied
 * @author Attila Szegedi
 */
class ProxyFactory[T <: AnyRef](implicit manifest: Manifest[T]) {
  val ctor = getProxyConstructor(manifest)

  private def getProxyConstructor(manifest: Manifest[T]) = {
    val cls = manifest.erasure

    val interfaces = if (cls.isInterface()) Array(cls) else cls.getInterfaces
    val clazz = reflect.Proxy.getProxyClass(cls.getClassLoader, interfaces: _*)
    clazz.getConstructor(classOf[reflect.InvocationHandler])
  }

  /**
   * Creates a proxy for the specific object where each method call on the
   * proxy is sent to the specified function.
   * @param obj the object being proxied
   * @param f the function receiving each method call
   * @tparam I the exposed interface of the created proxy. Must be an
   * interface the object implements.
   * @return a proxy for the object, implementing the requested interface,
   * that sends each method invocation through the specified function.
   */
  def apply[I >: T](obj: T)(f: Proxy.MethodCall[T] => Object): I = {
    val invocationHandler = new reflect.InvocationHandler {
      def invoke(unused: Object, method: reflect.Method, args: Array[Object]) = {
        f(new Proxy.MethodCall(obj, method, args))
      }
    }

    ctor.newInstance(invocationHandler).asInstanceOf[I]
  }
}
