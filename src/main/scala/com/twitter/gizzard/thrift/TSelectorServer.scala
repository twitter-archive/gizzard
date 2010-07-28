package com.twitter.gizzard.thrift

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels._
import java.util.concurrent.{ConcurrentLinkedQueue, LinkedBlockingQueue, ThreadPoolExecutor,
  TimeoutException, TimeUnit}
import scala.collection.jcl
import scala.collection.mutable
import org.apache.thrift._
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import org.apache.thrift.server._
import com.twitter.ostrich.Stats
import com.twitter.xrayspecs.{Duration, Time}
import com.twitter.xrayspecs.TimeConversions._
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger


object TSelectorServer {
  val log = Logger.get(getClass.getName)

  val cache = new mutable.HashMap[String, ThreadPoolExecutor]()

  def makeThreadPoolExecutor(config: ConfigMap): ThreadPoolExecutor = {
    val name = config("name")
    cache.get(name) foreach { executor =>
      if (!executor.isShutdown()) {
        return executor
      }
      cache.removeKey(name)
    }

    val stopTimeout = config.getInt("stop_timeout", 60)
    val minThreads = config("min_threads").toInt
    val maxThreads = config.getInt("max_threads", Math.MAX_INT)
    val queue = new LinkedBlockingQueue[Runnable]
    val executor = new ThreadPoolExecutor(minThreads, maxThreads, stopTimeout, TimeUnit.SECONDS,
                                          queue, new NamedPoolThreadFactory(name))
    cache(name) = executor
    executor
  }

  def apply(name: String, port: Int, processor: TProcessor, executor: ThreadPoolExecutor,
            timeout: Duration, idleTimeout: Duration): TSelectorServer = {
    val socket = ServerSocketChannel.open()
    socket.socket().setReuseAddress(true)
    socket.socket().bind(new InetSocketAddress(port), 8192)
    log.info("Starting %s (%s) on port %d", name, processor.getClass.getName, port)
    new TSelectorServer(name, processor, socket, executor, timeout, idleTimeout)
  }

  def apply(name: String, port: Int, config: ConfigMap, processor: TProcessor): TSelectorServer = {
    val clientTimeout = config("client_timeout_msec").toInt.milliseconds
    val idleTimeout = config("idle_timeout_sec").toInt.seconds
    apply(name, port, processor, makeThreadPoolExecutor(config), clientTimeout, idleTimeout)
  }
}

class TSelectorServer(name: String, processor: TProcessor, serverSocket: ServerSocketChannel,
                      executor: ThreadPoolExecutor, timeout: Duration, idleTimeout: Duration)
      extends TServer(null, null) {
  val log = Logger.get(getClass.getName)

  val processorFactory = new TProcessorFactory(processor)
  val inputTransportFactory = new TTransportFactory()
  val outputTransportFactory = new TTransportFactory()
  val inputProtocolFactory = new TBinaryProtocol.Factory(true, true)
  val outputProtocolFactory = new TBinaryProtocol.Factory(true, true)

  val clientTimeout = 0

  @volatile private var running = false
  var selectorThread: Thread = null

  case class Client(socketChannel: SocketChannel, processor: TProcessor, inputProtocol: TProtocol,
                    outputProtocol: TProtocol, var activity: Time)
  val clientMap = new mutable.HashMap[SelectableChannel, Client]
  val registerQueue = new ConcurrentLinkedQueue[SocketChannel]


  Stats.makeGauge("thrift-" + name + "-worker-threads") { executor.getPoolSize().toDouble }
  Stats.makeGauge("thrift-" + name + "-connections") { clientMap.synchronized { clientMap.size } }
  Stats.makeGauge("thrift-" + name + "-queue-size") { executor.getQueue().size() }

  def isRunning = running

  def execute(f: => Unit)(onTimeout: => Unit) {
    executor.execute(new Runnable() {
      val startTime = Time.now

      def run() {
        if (Time.now - startTime > timeout) {
          Stats.incr("thrift-" + name + "-timeout")
          onTimeout
        } else {
          f
        }
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

  def shutdown() {
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

    var lastScan = Time.now

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
            System.exit(1)
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

      // kill off any idle sockets
      if (Time.now - lastScan >= 1.second) {
        lastScan = Time.now
        val toRemove = new mutable.ListBuffer[SelectableChannel]
        clientMap.synchronized {
          for ((socket, client) <- clientMap) {
            if (lastScan - client.activity > idleTimeout) {
              toRemove += socket
            }
          }
          toRemove.foreach { socket =>
            val key = socket.keyFor(selector)
            if (key ne null) {
              key.cancel()
              closeSocket(socket)
            }
          }
        }
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
              client.activity = Time.now
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
          } {
            // if the job spent too long waiting for a thread:
            val client = clientMap.synchronized { clientMap(key.channel) }
            log.debug("Killing session (enqueued too long): %s", client.socketChannel)
            try {
              client.socketChannel.configureBlocking(true)
              new TApplicationException("server is too busy").write(client.outputProtocol)
            } finally {
              closeSocket(client.socketChannel)
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
        clientMap(clientSocket) = Client(clientSocket, processor, inputProtocol, outputProtocol,
                                         Time.now)
      }
    }

    def closeSocket(socket: SelectableChannel) {
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
