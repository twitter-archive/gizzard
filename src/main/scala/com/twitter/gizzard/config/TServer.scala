package com.twitter.gizzard.config

import com.twitter.util.Duration
import org.apache.thrift

trait ThreadPool {
  def name: String
  def stopTimeout: Int = 60
  def minThreads: Int
  def maxThreads: Int = Math.MAX_INT

  def apply() = {
    gizzard.thrift.TSelectorServer.makeThreadPoolExecutor(name, stopTimeout, minThreads, maxThreads)
  }
}

trait TNonblockingServer {
  def port: Int
  def timeout: Duration
  def idleTimeout: Duration
  def threadPool: ThreadPool

  def apply(processor: thrift.server.TNonblockingServer)
}

trait TSelectorServer extends TNonblockingServer

trait THsHaServer extends TNonblockingServer {
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
