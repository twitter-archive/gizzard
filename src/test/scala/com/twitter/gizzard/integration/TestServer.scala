package com.twitter.gizzard.testserver

import java.sql.{ResultSet, SQLException}
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.config.Connection
import com.twitter.querulous.query.SqlQueryTimeoutException
import gizzard.GizzardServer
import com.twitter.gizzard.scheduler.{CopyDestination, CopyDestinationShard}
import nameserver.NameServer
import shards.{ShardId, ShardInfo, ShardException, ShardTimeoutException}
import scheduler.{JobScheduler, JsonJob, CopyJob, CopyJobParser, CopyJobFactory, JsonJobParser, PrioritizingJobScheduler}

object config {
  import com.twitter.gizzard.config._
  import com.twitter.querulous.config._
  import com.twitter.util.TimeConversions._
  import com.twitter.util.Duration

  trait TestDBConnection extends Connection {
    val username = "root"
    val password = ""
    val hostnames = Seq("localhost")
  }

  object TestQueryEvaluator extends querulous.config.QueryEvaluator {
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
  }

  trait TestJobScheduler extends Scheduler {
    val schedulerType = new KestrelScheduler {
      val queuePath = "/tmp"
      override val keepJournal = false
    }
    errorLimit = 25
  }

  class TestNameServer(name: String) extends gizzard.config.NameServer {
    jobRelay.priority = Priority.Low.id

    val replicas = Seq(new Mysql {
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
        val server = new TestTHsHaServer { val name = "TestGizzardService"; val port = sPort }
        val databaseConnection = new TestDBConnection { val database = "gizzard_test_" + name }
        val nameServer = new TestNameServer(name)
        val jobQueues = Map(
          Priority.High.id -> new TestJobScheduler { val name = queueBase+"_high" },
          Priority.Low.id  -> new TestJobScheduler { val name = queueBase+"_low" }
        )

        jobInjector.port = iPort
        manager.port     = mPort
      }
    }

    def apply(name: String, port: Int): TestServer = apply(name, port, port + 1, port + 2)
  }
}


object Priority extends Enumeration {
  val High, Low = Value
}

class TestServer(conf: config.TestServer) extends GizzardServer[TestShard, JsonJob](conf) {

  // shard/nameserver/scheduler wiring

  val readWriteShardAdapter = new TestReadWriteAdapter(_)
  val jobPriorities         = List(Priority.High.id, Priority.Low.id)
  val copyPriority          = Priority.Low.id
  val copyFactory           = new TestCopyFactory(nameServer, jobScheduler(Priority.Low.id))

  shardRepo += ("TestShard" -> new SqlShardFactory(conf.queryEvaluator(), conf.databaseConnection))

  jobCodec += ("Put".r  -> new PutParser(nameServer.findCurrentForwarding(0, _)))
  jobCodec += ("Copy".r -> new TestCopyParser(nameServer, jobScheduler(Priority.Low.id)))


  // service listener

  val testService = new TestServerIFace(nameServer.findCurrentForwarding(0, _), jobScheduler)

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

class TestServerIFace(forwarding: Long => TestShard, scheduler: PrioritizingJobScheduler[JsonJob])
extends thrift.TestServer.Iface {
  import com.twitter.gizzard.thrift.conversions.Sequences._

  def put(key: Int, value: String) {
    scheduler.put(Priority.High.id, new PutJob(key, value, forwarding))
  }

  def get(key: Int) = forwarding(key).get(key).map(asTestResult).map(List(_).toJavaList) getOrElse List[thrift.TestResult]().toJavaList

  private def asTestResult(t: (Int, String, Int)) = new thrift.TestResult(t._1, t._2, t._3)
}


// Shard Definitions

trait TestShard extends shards.Shard {
  def put(key: Int, value: String): Unit
  def putAll(kvs: Seq[(Int, String)]): Unit
  def get(key: Int): Option[(Int, String, Int)]
  def getAll(key: Int, count: Int): Seq[(Int, String, Int)]
}

class TestReadWriteAdapter(s: shards.ReadWriteShard[TestShard])
extends shards.ReadWriteShardAdapter(s) with TestShard {
  def put(k: Int, v: String)         = s.writeOperation(_.put(k,v))
  def putAll(kvs: Seq[(Int,String)]) = s.writeOperation(_.putAll(kvs))
  def get(k: Int)                    = s.readOperation(_.get(k))
  def getAll(k:Int, c: Int)          = s.readOperation(_.getAll(k,c))
}

class SqlShardFactory(qeFactory: QueryEvaluatorFactory, conn: Connection)
extends shards.ShardFactory[TestShard] {
  def instantiate(info: ShardInfo, weight: Int, children: Seq[TestShard]) =
    new SqlShard(qeFactory(conn.withHost(info.hostname)), info, weight, children)

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

class SqlShard(
  evaluator: QueryEvaluator,
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[TestShard])
extends TestShard {
  private val table = shardInfo.tablePrefix

  private val putSql = """insert into %s (id, value, count) values (?,?,1) on duplicate key
                          update value = values(value), count = count+1""".format(table)
  private val getSql    = "select * from " + table + " where id = ?"
  private val getAllSql = "select * from " + table + " where id > ? limit ?"

  private def asResult(r: ResultSet) = (r.getInt("id"), r.getString("value"), r.getInt("count"))

  def put(key: Int, value: String) { evaluator.execute(putSql, key, value) }
  def putAll(kvs: Seq[(Int, String)]) {
    evaluator.executeBatch(putSql) { b => for ((k,v) <- kvs) b(k,v) }
  }

  def get(key: Int) = evaluator.selectOne(getSql, key)(asResult)
  def getAll(key: Int, count: Int) = evaluator.select(getSql, key, count)(asResult)
}


// Jobs

class PutParser(forwarding: Long => TestShard) extends JsonJobParser {
  def apply(map: Map[String, Any]): JsonJob = {
    new PutJob(map("key").asInstanceOf[Int], map("value").asInstanceOf[String], forwarding)
  }
}

class PutJob(key: Int, value: String, forwarding: Long => TestShard) extends JsonJob {
  def toMap = Map("key" -> key, "value" -> value)
  def apply() { forwarding(key).put(key, value) }
}

class TestCopyFactory(ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJobFactory[TestShard] {
  def apply(src: ShardId, dests: List[CopyDestination]) = new TestCopy(src, dests, 0, 500, ns, s)
}

class TestCopyParser(ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJobParser[TestShard] {
  def deserialize(m: Map[String, Any], src: ShardId, dests: List[CopyDestination], count: Int) = {
    val cursor = m("cursor").asInstanceOf[Int]
    val count  = m("count").asInstanceOf[Int]
    new TestCopy(src, dests, cursor, count, ns, s)
  }
}

class TestCopy(srcId: ShardId, destinations: List[CopyDestination], cursor: Int, count: Int,
               ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJob[TestShard](srcId, destinations, count, ns, s) {
  def copyPage(src: TestShard, dests: List[CopyDestinationShard[TestShard]], count: Int) = {
    val rows = src.getAll(cursor, count).map { case (k,v,c) => (k,v) }

    dests.foreach(_.shard.putAll(rows))

    if (rows.isEmpty) None
    else Some(new TestCopy(srcId, destinations, rows.last._1, count, ns, s))
  }

  def serialize = Map("cursor" -> cursor)
}
