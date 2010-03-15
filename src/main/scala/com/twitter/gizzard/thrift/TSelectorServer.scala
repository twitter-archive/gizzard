package com.twitter.gizzard.thrift

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels._
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue, ThreadPoolExecutor,
  TimeoutException, TimeUnit}
import scala.collection.jcl
import scala.collection.mutable
import com.facebook.thrift._
import com.facebook.thrift.protocol._
import com.facebook.thrift.transport._
import com.facebook.thrift.server._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.{Duration, Time}
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


object TSelectorServer {
  val log = Logger.get(getClass.getName)

  def makeThreadPoolExecutor(config: ConfigMap) = {
    val stopTimeout = config.getInt("stop_timeout", 60)
    val minThreads = config("min_threads").toInt
    val maxThreads = config.getInt("max_threads", Math.MAX_INT)
    val queue = new LinkedBlockingQueue[Runnable]
    new ThreadPoolExecutor(minThreads, maxThreads, stopTimeout, TimeUnit.SECONDS, queue)
  }

  def apply(name: String, port: Int, processor: TProcessor, executor: ThreadPoolExecutor,
            timeout: Duration) = {
    val socket = ServerSocketChannel.open()
    socket.socket().setReuseAddress(true)
    socket.socket().bind(new InetSocketAddress(port), 8192)
    log.info("Starting %s (%s) on port %d", name, processor.getClass.getName, port)
    new TSelectorServer(name, processor, socket, executor, timeout)
  }
}

class TSelectorServer(name: String, processor: TProcessor, serverSocket: ServerSocketChannel,
                      executor: ThreadPoolExecutor, timeout: Duration) extends TServer(null, null) {
  val log = Logger.get(getClass.getName)

  val processorFactory = new TProcessorFactory(processor)
  val inputTransportFactory = new TTransportFactory()
  val outputTransportFactory = new TTransportFactory()
  val inputProtocolFactory = new TBinaryProtocol.Factory(true, true)
  val outputProtocolFactory = new TBinaryProtocol.Factory(true, true)

  val clientTimeout = 0

  @volatile private var running = false
  var selectorThread: Thread = null

  case class Client(socketChannel: SocketChannel, processor: TProcessor, inputProtocol: TProtocol, outputProtocol: TProtocol)
  val clientMap = new mutable.HashMap[SelectableChannel, Client]
  val registerQueue = new ConcurrentLinkedQueue[SocketChannel]


  Stats.makeGauge("thrift-" + name + "-worker-threads") { executor.getPoolSize().toDouble }
  Stats.makeGauge("thrift-" + name + "-connections") { clientMap.synchronized { clientMap.size } }
  Stats.makeGauge("thrift-" + name + "-queue-size") { executor.getQueue().size() }

  def isRunning = running

  def execute(f: => Unit) {
    executor.execute(new Runnable() {
      val startTime = Time.now

      def run() {
        if (Time.now - startTime > timeout) {
          Stats.incr("thrift-" + name + "-timeout")
          throw new TimeoutException("thrift connection spent too long in queue")
        }
        f
      }
    })
  }

  def serve() {
    try {
      serverSocket.socket().setSoTimeout(0)
    } catch {
      case e: IOException => log.warning(e, "Could not set socket timeout.")
    }

    selectorThread = new SelectorThread()
    selectorThread.start()
  }

  override def stop() {
    running = false
    selectorThread.join()
    try {
      serverSocket.close()
    } catch {
      case _ =>
    }

    executor.shutdown()
    while (!executor.isTerminated()) {
      log.info("Waiting for thread-pool executor...")
      try {
        executor.awaitTermination(1, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
      }
    }
  }


  class SelectorThread extends Thread("SelectorThread") {
    val selector = Selector.open()
    serverSocket.configureBlocking(false)
    serverSocket.register(selector, SelectionKey.OP_ACCEPT)

    override def run() {
      running = true
      var errorCount = 0
      while (running) {
        try {
          select()
          errorCount = 0
        } catch {
          case e: IOException =>
            log.error(e, "I/O exception in select: %s", e)
            errorCount += 1
            if (errorCount > 10) {
              log.error(e, "Too many select errors. Dying...")
              // a server with an open thrift-server socket but no thread to handle connections is useless.
              System.exit(1)
            }
          case e: Exception =>
            log.error(e, "Unexpected exception! Dying...")
            throw e
        }
      }
    }

    def select() {
      var channel = registerQueue.poll()
      while (channel ne null) {
        channel.configureBlocking(false)
        channel.register(selector, SelectionKey.OP_READ)
        channel = registerQueue.poll()
      }

      selector.select(100)

      for (key <- jcl.Set(selector.selectedKeys)) {
        if (key.isAcceptable()) {
          // there's only one listen socket for now.
          val clientSocket = serverSocket.accept()
//          clientSocket.socket().setTcpNoDelay(true)
          clientSocket.configureBlocking(false)
          clientSocket.register(selector, SelectionKey.OP_READ)
          addSession(clientSocket)
        } else {
          key.cancel()
          execute {
            val (_, duration) = Stats.duration {
              val client = clientMap.synchronized { clientMap(key.channel) }
              try {
                client.socketChannel.configureBlocking(true)
                client.processor.process(client.inputProtocol, client.outputProtocol)
                Stats.incr("thrift-" + name + "-calls")
                registerQueue.add(client.socketChannel)
                selector.wakeup()
              } catch {
                case e: TTransportException =>
                  // session ends
                  closeSocket(client.socketChannel)
                case e: Throwable =>
                  log.error(e, "Exception in client processor")
                  closeSocket(client.socketChannel)
              }
            }
            if (duration > 50) {
              Stats.incr("thrift-" + name + "-work-50")
            }
          }
        }
      }
      selector.selectedKeys.clear()
      selector.selectNow()
    }

    def addSession(clientSocket: SocketChannel) {
      val transport = new TSocket(clientSocket.socket())
      transport.setTimeout(clientTimeout)
      log.debug("Start of session: %s", clientSocket)

      // thrift gibberish.
      val processor = processorFactory.getProcessor(transport)
      val inputProtocol = inputProtocolFactory.getProtocol(inputTransportFactory.getTransport(transport))
      val outputProtocol = outputProtocolFactory.getProtocol(inputTransportFactory.getTransport(transport))

      clientMap.synchronized {
        clientMap(clientSocket) = Client(clientSocket, processor, inputProtocol, outputProtocol)
      }
    }

    def closeSocket(socket: SocketChannel) {
      log.debug("End of session: %s", socket)
      try {
        socket.close()
      } catch {
        case _ =>
      }
      clientMap.synchronized { clientMap -= socket }
    }
  }
}
