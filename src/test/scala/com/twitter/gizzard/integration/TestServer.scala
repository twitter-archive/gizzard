package com.twitter.gizzard
package testserver

import java.sql.{ResultSet, SQLException}
import com.twitter.querulous
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.config.Connection
import com.twitter.querulous.query.SqlQueryTimeoutException
import collection.mutable.ListBuffer

import com.twitter.gizzard
import nameserver.NameServer
import shards.{ShardId, ShardInfo, ShardException, ShardTimeoutException, Cursorable}
import scheduler.{JobScheduler, JsonJob, JsonJobParser, PrioritizingJobScheduler, Repairable, MultiShardRepair, RepairJobFactory, RepairJobParser}

package object config {
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
      path = "/tmp"
      keepJournal = false
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

        def repairPriority = Priority.High.id

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

class TestServer(conf: config.TestServer) extends GizzardServer[TestShard](conf) {

  // shard/nameserver/scheduler wiring

  val readWriteShardAdapter = new TestReadWriteAdapter(_)
  val jobPriorities         = List(Priority.High.id, Priority.Low.id)

  def repairPriority          = Priority.High.id
  val repairFactory           = new TestRepairFactory(nameServer, jobScheduler)

  shardRepo += ("TestShard" -> new SqlShardFactory(conf.queryEvaluator(), conf.databaseConnection))

  jobCodec += ("Repair".r -> new TestRepairParser(nameServer, jobScheduler))
  jobCodec += ("Put".r  -> new PutParser(nameServer.findCurrentForwarding(0, _)))

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

class TestServerIFace(forwarding: Long => TestShard, scheduler: PrioritizingJobScheduler)
extends thrift.TestServer.Iface {
  import scala.collection.JavaConversions._
  import com.twitter.gizzard.thrift.conversions.Sequences._

  def put(key: Int, value: String) {
    scheduler.put(Priority.High.id, new PutJob(key, value, forwarding))
  }

  def get(key: Int) = forwarding(key).get(key).toList.map(asTestResult)

  private def asTestResult(t: TestResult) = new thrift.TestResult(t.id, t.value, t.count)
}


// Shard Definitions

case class TestResult(id: Int, value: String, count: Int) extends Repairable[TestResult] {
  def similar(other: TestResult) = {
    id.compare(other.id)
  }
  def shouldRepair(other: TestResult) = {
    similar(other) == 0 && value != other.value
  }
}

object TestCursor {
  val StartPosition = -1
  val EndPosition = 0
  val Start = new TestCursor(StartPosition)
  val End = new TestCursor(EndPosition)
}

case class TestCursor(position: Int) extends Cursorable[TestCursor] {
  def atStart = position == TestCursor.StartPosition
  def atEnd = position == TestCursor.EndPosition
  def compare(other: TestCursor) = {
    (atEnd, other.atEnd) match {
      case (true, true) => 0
      case (true, false) => 1
      case (false, true) => -1
      case _ => position.compare(other.position)
    }
  }
}

trait TestShard extends shards.Shard {
  def put(key: Int, value: String): Unit
  def putAll(kvs: Seq[(Int, String)]): Unit
  def get(key: Int): Option[TestResult]
  def getAll(key: Int, count: Int): (Seq[TestResult], TestCursor)
  def getAll(key: TestCursor, count: Int): (Seq[TestResult], TestCursor)
}

class TestReadWriteAdapter(s: shards.ReadWriteShard[TestShard])
extends shards.ReadWriteShardAdapter(s) with TestShard {
  def put(k: Int, v: String)         = s.writeOperation(_.put(k,v))
  def putAll(kvs: Seq[(Int,String)]) = s.writeOperation(_.putAll(kvs))
  def get(k: Int)                    = s.readOperation(_.get(k))
  def getAll(k:Int, c: Int)          = s.readOperation(_.getAll(k,c))
  def getAll(k:TestCursor, c: Int)   = s.readOperation(_.getAll(k,c))
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

  private def asResult(r: ResultSet) = new TestResult(r.getInt("id"), r.getString("value"), r.getInt("count"))

  override def toString = shardInfo.toString
  def put(key: Int, value: String) { evaluator.execute(putSql, key, value) }
  def putAll(kvs: Seq[(Int, String)]) {
    evaluator.executeBatch(putSql) { b => for ((k,v) <- kvs) b(k,v) }
  }

  def get(key: Int) = evaluator.selectOne(getSql, key)(asResult)
  def getAll(key: Int, count: Int) = {
    val result = evaluator.select(getAllSql, key, count)(asResult)
    (result, if (result.size != count) TestCursor.End else new TestCursor(key + count))
  }

  def getAll(key: TestCursor, count: Int) = getAll(key.position, count)
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

// class MetadataRepair(shardIds: Seq[ShardId], cursor: Cursor, count: Int,
//     nameServer: NameServer[Shard], scheduler: PrioritizingJobScheduler)
//   extends MultiShardRepair[Shard, Metadata, Cursor](shardIds, cursor, count, nameServer, scheduler, Repair.PRIORITY) {

class TestRepairFactory(ns: NameServer[TestShard], s: PrioritizingJobScheduler)
extends RepairJobFactory[TestShard] {
  def apply(shardIds: Seq[ShardId]) = {
    new TestRepair(shardIds, TestCursor.Start, 500, ns, s)
  }
}

class TestRepair(shardIds: Seq[ShardId], cursor: TestCursor, count: Int,
    nameServer: NameServer[TestShard], scheduler: PrioritizingJobScheduler) extends MultiShardRepair[TestShard, TestResult, TestCursor](shardIds, cursor, count, nameServer, scheduler, Priority.High.id) {

  def select(shard: TestShard, cursor: TestCursor, count: Int) = shard.getAll(cursor, count)
  def scheduleBulk(otherShards: Seq[TestShard], items: Seq[TestResult]) = { 
    otherShards.foreach(_.putAll(items.map{i => (i.id, i.value)}))
  }
  def scheduleItem(missing: Boolean, list: (TestShard, ListBuffer[TestResult], TestCursor), tableId: Int, item: TestResult) = {
    scheduler.put(Priority.High.id, new PutJob(item.id, item.value, nameServer.findCurrentForwarding(0, _)))
  }
  def nextRepair(lowestCursor: TestCursor) = {
    if (lowestCursor.atEnd) None else Some(new TestRepair(shardIds, lowestCursor, count, nameServer, scheduler))
  }
  def serialize = {
    Map("cursor" -> cursor.position)
  }
}

class TestRepairParser(ns: NameServer[TestShard], s: PrioritizingJobScheduler)
extends RepairJobParser[TestShard] {
  def deserialize(m: Map[String, Any], shardIds: Seq[ShardId], count: Int) = {
    val cursor = new TestCursor(m("cursor").asInstanceOf[Int])
    new TestRepair(shardIds, cursor, count, ns, s)
  }
}
