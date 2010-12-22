package com.twitter.gizzard.thrift

import com.twitter.rpcclient.{PooledClient, ThriftConnection}
import com.twitter.util.Duration

class JobInjectorClient(host: String, port: Int, framed: Boolean, soTimeout: Duration, retryCount: Int)
  extends PooledClient[JobInjector.Iface] {

  def this(host: String, port: Int, framed: Boolean, soTimeout: Duration) =
    this(host, port, framed, soTimeout, 0)

  val name = "JobManagerClient"

  def createConnection =
    new ThriftConnection[JobInjector.Client](host, port, framed) {
      override def SO_TIMEOUT = soTimeout
    }
}
