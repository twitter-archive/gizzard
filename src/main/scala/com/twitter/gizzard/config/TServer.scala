package com.twitter.gizzard.config

import java.util.concurrent.ThreadPoolExecutor
import com.twitter.util.Duration
import org.apache.thrift

trait ThreadPool extends (() => ThreadPoolExecutor) {
  def name: String
  def stopTimeout: Int = 60
  def minThreads: Int
  def maxThreads: Int = minThreads

  def apply(): ThreadPoolExecutor = {
    gizzard.thrift.TSelectorServer.makeThreadPoolExecutor(name, stopTimeout, minThreads, maxThreads)
  }
}

trait TServer extends (thrift.TProcessor => thrift.server.TServer) {
  def port: Int
  def timeout: Duration
  def idleTimeout: Duration
  def threadPool: ThreadPool

  def apply(processor: thrift.TProcessor): thrift.server.TServer
}

trait TSelectorServer extends TServer {
  def name: String

  def apply(processor: thrift.TProcessor) = {
    gizzard.thrift.TSelectorServer(name, port, processor, threadPool(), timeout, idleTimeout)
  }
}

trait TThreadServer extends TServer {
  def name: String

  def apply(processor: thrift.TProcessor) = {
    gizzard.thrift.TThreadServer(name, port, idleTimeout.inMillis.toInt, threadPool(), processor)
  }
}

trait THsHaServer extends TServer {
  def apply(processor: thrift.TProcessor) = {
    val transport = new thrift.transport.TNonblockingServerSocket(port, timeout.inMillis.toInt)
    val options   = new thrift.server.TNonblockingServer.Options
    new thrift.server.THsHaServer(
      new thrift.TProcessorFactory(processor),
      transport,
      new thrift.transport.TFramedTransport.Factory(),
      new thrift.protocol.TBinaryProtocol.Factory(),
      new thrift.protocol.TBinaryProtocol.Factory(),
      threadPool(),
      options)
  }
}
