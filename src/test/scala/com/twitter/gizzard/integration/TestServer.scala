package com.twitter.gizzard.testserver

import java.sql.{ResultSet, SQLException}
import java.io.File
import com.twitter.querulous
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.config.Connection
import com.twitter.querulous.query.SqlQueryTimeoutException

import com.twitter.gizzard
import com.twitter.gizzard.GizzardServer
import com.twitter.gizzard.nameserver.{NameServer, Forwarder}
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._


package object config {
  import com.twitter.logging.config._
  import com.twitter.gizzard.config._
  import com.twitter.gizzard.logging.config._
  import com.twitter.querulous.config._
  import com.twitter.conversions.time._
  import com.twitter.util.Duration

  trait TestDBConnection extends Connection {
    val username = "root"
    val password = ""
    val hostnames = Seq("localhost")
  }

  object TestQueryEvaluator extends querulous.config.QueryEvaluator {
    singletonFactory = true
    database.memoize = true
    database.pool = new ApachePoolingDatabase {
      sizeMin = 3
      sizeMax = 3
    }
  }

  trait TestTHsHaServer extends THsHaServer {
    threadPool.minThreads = 10
  }

  trait TestServer extends gizzard.config.GizzardServer {
    def server: TServer
    def databaseConnection: Connection

    val queryEvaluator = TestQueryEvaluator
    jobRelay.priority = Priority.Low.id

    loggers = List(
      new LoggerConfig {
        level = Level.ERROR
      }, new LoggerConfig {
        node = "w3c"
        useParents = false
        level = Level.DEBUG
      }, new LoggerConfig {
        node = "bad_jobs"
        useParents = false
        level = Level.INFO
      },
      new LoggerConfig {
        node = "exception"
        useParents = false
        level = Some(Level.INFO)
        handlers = List(new FileHandlerConfig {
          formatter = new ExceptionJsonFormatterConfig
          filename = "./exception.log"
        })
      }
    )
  }

  trait TestJobScheduler extends Scheduler {
    val schedulerType = new KestrelScheduler {
      path = "/tmp"
      keepJournal = false
    }
    errorLimit = 25
  }

  private def testNameServerReplicas(name: String) = {
    Seq(new Mysql {
      queryEvaluator = TestQueryEvaluator
      val connection = new TestDBConnection {
        val database = "gizzard_test_" + name + "_ns"
      }
    })
  }


  object TestServerConfig {
    def apply(name: String, sPort: Int, iPort: Int, mPort: Int) = {
      val queueBase = "gizzard_test_" + name

      new TestServer {
        mappingFunction    = Identity
        nameServerReplicas = testNameServerReplicas(name)
        jobInjector.port   = iPort
        manager.port       = mPort
        jobAsyncReplicator.path = "/tmp/" + queueBase + "_replication"

        val server             = new TestTHsHaServer { val name = "TestGizzardService"; val port = sPort }
        val databaseConnection = new TestDBConnection { val database = "gizzard_test_" + name }
        val jobQueues = Map(
          Priority.High.id -> new TestJobScheduler { val name = queueBase+"_high" },
          Priority.Low.id  -> new TestJobScheduler { val name = queueBase+"_low" }
        )
      }
    }

    def apply(name: String, port: Int): TestServer = apply(name, port, port + 1, port + 2)
  }
}


object Priority extends Enumeration {
  val High, Low = Value
}

class TestServer(conf: config.TestServer) extends GizzardServer(conf) {

  // shard/nameserver/scheduler wiring
  val jobPriorities         = List(Priority.High.id, Priority.Low.id)
  val copyPriority          = Priority.Low.id

  nameServer.configureForwarder[TestShard](
    _.tableId(0)
    .shardFactory(new TestShardFactory(conf.queryEvaluator(), conf.databaseConnection))
    .copyFactory(new TestCopyFactory(nameServer, jobScheduler(Priority.Low.id)))
  )

  jobCodec += ("Put".r  -> new PutParser(nameServer.forwarder[TestShard]))
  jobCodec += ("Copy".r -> new TestCopyParser(nameServer, jobScheduler(Priority.Low.id)))


  // service listener

  val testService = new TestServerIFace(nameServer.forwarder[TestShard], jobScheduler)

  lazy val testThriftServer = {
    val processor = new thrift.TestServer.Processor(testService)
    conf.server(processor)
  }

  def start() {
    startGizzard()
    new Thread(new Runnable { def run() { testThriftServer.serve() } }, "TestServerThread").start()
  }

  def shutdown(quiesce: Boolean) {
    testThriftServer.stop()
    shutdownGizzard(quiesce)
  }
}


// Service Interface

class TestServerIFace(forwarding: Long => RoutingNode[TestShard], scheduler: PrioritizingJobScheduler)
extends thrift.TestServer.Iface {
  import scala.collection.JavaConversions._
  import com.twitter.gizzard.thrift.conversions.Sequences._

  def put(key: Int, value: String) {
    scheduler.put(Priority.High.id, new PutJob(key, value, forwarding))
  }

  def get(key: Int) = forwarding(key).read.any(_.get(key)).toList.map(asTestResult)

  private def asTestResult(t: (Int, String, Int)) = new thrift.TestResult(t._1, t._2, t._3)
}


// Shard Definitions

class TestShardFactory(qeFactory: QueryEvaluatorFactory, conn: Connection) extends ShardFactory[TestShard] {
  def newEvaluator(host: String) = qeFactory(conn.withHost(host))

  def instantiate(info: ShardInfo, weight: Weight) = new TestShard(newEvaluator(info.hostname), info, false)

  def instantiateReadOnly(info: ShardInfo, weight: Weight) = instantiate(info, weight)

  def materialize(info: ShardInfo) {
    val ddl =
      """create table if not exists %s (
           id int(11) not null,
           value varchar(255) not null,
           count int(11) not null default 1,
           primary key (id)
         ) engine=innodb default charset=utf8"""
    try {
      val e = qeFactory(conn.withHost(info.hostname).withoutDatabase)
      e.execute("create database if not exists " + conn.database)
      e.execute(ddl.format(conn.database + "." + info.tablePrefix))
    } catch {
      case e: SQLException             => throw new ShardException(e.toString)
      case e: SqlQueryTimeoutException => throw new ShardTimeoutException(e.timeout, info.id)
    }
  }
}

// should enforce read/write perms at the db access level
class TestShard(evaluator: QueryEvaluator, val shardInfo: ShardInfo, readOnly: Boolean) {

  private val table = shardInfo.tablePrefix

  private val putSql  = """insert into %s (id, value, count) values (?,?,1) on duplicate key
                           update value = values(value), count = count+1""".format(table)
  private val getSql    = "select * from " + table + " where id = ?"
  private val getAllSql = "select * from " + table + " where id > ? limit ?"

  private def asResult(r: ResultSet) = (r.getInt("id"), r.getString("value"), r.getInt("count"))

  def put(key: Int, value: String) {
    if (readOnly) error("shard is read only!")
    evaluator.execute(putSql, key, value)
  }

  def putAll(kvs: Seq[(Int, String)]) {
    if (readOnly) error("shard is read only!")
    evaluator.executeBatch(putSql) { b => for ((k,v) <- kvs) b(k,v) }
  }

  def get(key: Int) = evaluator.selectOne(getSql, key)(asResult)
  def getAll(key: Int, count: Int) = evaluator.select(getAllSql, key, count)(asResult)
}


// Jobs

class PutParser(forwarding: Long => RoutingNode[TestShard]) extends JsonJobParser {
  def apply(map: Map[String, Any]): JsonJob = {
    new PutJob(map("key").asInstanceOf[Int], map("value").asInstanceOf[String], forwarding)
  }
}

class PutJob(key: Int, value: String, forwarding: Long => RoutingNode[TestShard]) extends JsonJob {
  def toMap = Map("key" -> key, "value" -> value)
  def apply() { forwarding(key).write.foreach(_.put(key, value)) }
}

class TestCopyFactory(ns: NameServer, s: JobScheduler)
extends CopyJobFactory[TestShard] {
  def apply(shardIds: Seq[ShardId]) = new TestCopy(shardIds, 0, 500, ns, s)
}

class TestCopyParser(ns: NameServer, s: JobScheduler)
extends CopyJobParser[TestShard] {
  def deserialize(m: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = m("cursor").asInstanceOf[Int]
    val count  = m("count").asInstanceOf[Int]
    new TestCopy(shardIds, cursor, count, ns, s)
  }
}

class TestCopy(
  shardIds: Seq[ShardId],
  cursor: Int,
  count: Int,
  ns: NameServer,
  s: JobScheduler)
extends CopyJob[TestShard](shardIds, count, ns, s) {

  def copyPage(nodes: Seq[RoutingNode[TestShard]], count: Int) = {
    val rows = nodes.map { _.read.any(_.getAll(cursor, count)) map { case (k,v,c) => (k,v) }}.flatten

    if (rows.isEmpty) {
      None
    } else {
      nodes.map { _.write.foreach(_.putAll(rows)) }
      Some(new TestCopy(shardIds, rows.last._1, count, ns, s))
    }
  }

  def serialize = Map("cursor" -> cursor)
}
