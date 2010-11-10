package com.twitter.gizzard.thrift

import java.net.{ServerSocket, Socket, SocketTimeoutException}
import java.util.concurrent.{CountDownLatch, ExecutorService, SynchronousQueue, ThreadPoolExecutor, TimeUnit}
import com.twitter.gizzard.NamedPoolThreadFactory
import com.twitter.ostrich.Stats
import net.lag.logging.Logger
import org.apache.thrift.{TProcessor, TProcessorFactory}
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol, TProtocolFactory}
import org.apache.thrift.server.TServer
import org.apache.thrift.transport.{TServerTransport, TSocket, TTransport, TTransportException, TTransportFactory}

object TThreadServer {
  private val MIN_THREADS = 5

  def apply(name: String, port: Int, idleTimeout: Int, executor: ExecutorService,
            processor: TProcessor): TThreadServer = {
    new TThreadServer(name, port, idleTimeout, executor, new TProcessorFactory(processor),
                      new TTransportFactory(), new TBinaryProtocol.Factory())
  }

  def apply(name: String, port: Int, idleTimeout: Int, processor: TProcessor): TThreadServer = {
    TThreadServer(name, port, idleTimeout, makeThreadPool(name, MIN_THREADS), processor)
  }

  def makeThreadPool(name: String, minThreads: Int): ExecutorService = {
    val queue = new SynchronousQueue[Runnable]
    val executor = new ThreadPoolExecutor(minThreads, Math.MAX_INT, 60, TimeUnit.SECONDS, queue,
      new NamedPoolThreadFactory(name))

    Stats.makeGauge("thrift-" + name + "-worker-threads") { executor.getPoolSize().toDouble }

    executor
  }
}

/*
 * Various improvements to the libthrift TThreadPoolServer:
 *
 * Each connection gets its own thread from a thread pool, with an idle timeout. You can pass your
 * own ExecutorService. On shutdown, it'll wait up to 5 seconds for all the worker threads to
 * finish up and realize they're dead. (Unlike the libthrift version, which can't be reliably
 * shut down.)
 */
class TThreadServer(name: String, port: Int, idleTimeout: Int,
                    val executor: ExecutorService,
                    processorFactory: TProcessorFactory,
                    transportFactory: TTransportFactory,
                    protocolFactory: TProtocolFactory)
      extends TServer(processorFactory, null, transportFactory, transportFactory,
                      protocolFactory, protocolFactory) {
  private val log = Logger(getClass.getName)

  private val ACCEPT_TIMEOUT = 1000
  private val SHUTDOWN_TIMEOUT = 5000

  @volatile var running = true
  private val deathSwitch = new CountDownLatch(1)

  def start() {
    new Thread(name) {
      override def run() {
        serve()
      }
    }.start()
  }

  def serve() {
    log.info("Starting thrift service %s on port %d.", name, port)

    val serverSocket = new ServerSocket(port)
    serverSocket.setReuseAddress(true)
    serverSocket.setSoTimeout(ACCEPT_TIMEOUT)

    while (running) {
      try {
        val client = serverSocket.accept()
        client.setSoTimeout(idleTimeout)
        executor.execute(new Runnable() {
          def run() {
            try {
              process(client)
            } catch {
              case x: Exception =>
                log.debug(x, "Client died prematurely: %s", x)
            }
          }
        })
      } catch {
        case x: SocketTimeoutException =>
          // ignore
        case x: Exception =>
          log.error(x, "Error occurred during accept: %s", x)
          running = false
      }
    }

    log.info("Shutting down thrift service %s on port %d.", name, port)

    serverSocket.close()
    executor.shutdown()
    executor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)
    executor.shutdownNow()

    log.info("Finished shutting down service %s.", name)
    deathSwitch.countDown()
  }

  override def stop() {
    running = false
    deathSwitch.await()
  }

  private def process(client: Socket) {
    val transport = new TSocket(client)
    val processor = processorFactory.getProcessor(transport)
    val protocol = protocolFactory.getProtocol(transportFactory.getTransport(transport))
    while (running && processor.process(protocol, protocol)) {
      // ...
    }
    try {
      client.close()
    } catch {
      case _ =>
    }
  }
}
