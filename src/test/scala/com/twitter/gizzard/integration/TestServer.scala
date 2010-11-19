package com.twitter.gizzard.testserver

import java.sql.{ResultSet, SQLException}
import com.twitter.querulous.evaluator.{QueryEvaluatorFactory, QueryEvaluator}
import com.twitter.querulous.config.Connection
import com.twitter.querulous.query.SqlQueryTimeoutException
import gizzard.GizzardServer
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
    val debug       = true
    val autoDisable = None
    val query       = new Query {}
    val database = new Database {
      val statsCollector = None
      val timeout        = None
      val pool = Some(new ApachePoolingDatabase {
        val testIdleMsec = 1.seconds
      })
    }
  }

  trait TestTHsHaServer extends THsHaServer {
    // val port     = 7919
    val timeout     = 100.millis
    val idleTimeout = 60.seconds

    val threadPool = new ThreadPool {
      val name       = "TestThriftServerThreadPool"
      val minThreads = 10
    }
  }

  trait TestServer extends gizzard.config.GizzardServer {
    def server: TServer
    def databaseConnection: Connection
    val queryEvaluator = TestQueryEvaluator
  }

  trait TestJobScheduler extends Scheduler {
    val schedulerType = new Kestrel {
      val queuePath = "/tmp"
      override val keepJournal = false
    }
    val threads           = 1
    val errorLimit        = 25
    val replayInterval    = 900.seconds
    val perFlushItemLimit = 1000
    val jitterRate        = 0.0f
  }

  object TestServer {
    def apply(name: String, sPort: Int, iPort: Int, mPort: Int) =
      new TestServer {
        val server           = new TestTHsHaServer { val port = sPort }
        val jobInjector      = new JobInjector with TestTHsHaServer { override val port = iPort }
        val nsQueryEvaluator = TestQueryEvaluator
        val databaseConnection = new TestDBConnection {
          val database = "gizzard_test_" + name
        }
        val nameServer = new gizzard.config.NameServer {
          override val jobRelay = None
          val mappingFunction   = Identity
          val replicas = Seq(new Mysql with TestDBConnection {
            val database = "gizzard_test_" + name + "_ns"
          })
        }
        val jobQueues = Map(
          Priority.High.id -> new TestJobScheduler { val name = "gizzard_test_high_queue" },
          Priority.Low.id  -> new TestJobScheduler { val name = "gizzard_test_low_queue" }
        )
        override val manager = new Manager with TThreadServer {
          override val port = mPort
          val threadPool   = new ThreadPool {
            val name       = "gizzard"
            val minThreads = 0
            override val maxThreads = 1
          }
        }
      }
  }
}



object Priority extends Enumeration {
  val High, Low = Value
}

class TestServer(conf: config.TestServer) extends GizzardServer[TestShard, JsonJob](conf) {

  //protected val log = Logger.get(getClass.getName)

  // job wiring

  val copyFactory   = new TestCopyFactory(nameServer, jobScheduler(Priority.Low.id))
  val badJobQueue   = None
  val jobPriorities = List(Priority.High.id, Priority.Low.id)
  val copyPriority  = Priority.Low.id
  def logUnparsableJob(j: Array[Byte]) {
    //log.error("Unparsable job: %s", j.map(b => "%02x".format(b.toInt & 0xff)).mkString(", "))
  }

  jobCodec += ("Put".r  -> new PutParser(nameServer.findCurrentForwarding(0, _)))
  jobCodec += ("Copy".r -> new TestCopyParser(nameServer, jobScheduler(Priority.Low.id)))


  // shard/nameserver wiring

  val readWriteShardAdapter = new TestReadWriteAdapter(_)
  val replicationFuture     = None

  shardRepo += ("TestShard" -> new SqlShardFactory(conf.queryEvaluator(), conf.databaseConnection))

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
  def put(key: Int, value: String) {
    scheduler.put(Priority.High.id, new PutJob(key, value, forwarding))
  }

  def get(key: Int) = forwarding(key).get(key).map(_._2) getOrElse null
}


// Shard Definitions

trait TestShard extends shards.Shard {
  def put(key: Int, value: String): Unit
  def putAll(kvs: Seq[(Int, String)]): Unit
  def get(key: Int): Option[(Int, String)]
  def getAll(key: Int, count: Int): Seq[(Int, String)]
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

  private val putSql    = """insert into " + table + " (id, value) values (?,?)
                             on duplicate key update value = values(value)"""
  private val getSql    = "select value from " + table + " where id in (?)"
  private val getAllSql = "select value from " + table + " where id > ? limit ?"

  private def asPair(r: ResultSet) = r.getInt("id") -> r.getString("value")

  def put(key: Int, value: String) { evaluator.execute(putSql, key, value) }
  def putAll(kvs: Seq[(Int, String)]) {
    evaluator.executeBatch(putSql) { b => for ((k,v) <- kvs) b(k,v) }
  }

  def get(key: Int) = evaluator.selectOne(getSql, key)(asPair)
  def getAll(key: Int, count: Int) = evaluator.select(getSql, key, count)(asPair)
}


// Jobs

class PutParser(forwarding: Long => TestShard) extends JsonJobParser[JsonJob] {
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
  def apply(src: ShardId, dest: ShardId) = new TestCopy(src, dest, 0, 500, ns, s)
}

class TestCopyParser(ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJobParser[TestShard] {
  def deserialize(m: Map[String, Any], src: ShardId, dest: ShardId, count: Int) = {
    val cursor = m("cursor").asInstanceOf[Int]
    val count  = m("count").asInstanceOf[Int]
    new TestCopy(src, dest, cursor, count, ns, s)
  }
}

class TestCopy(srcId: ShardId, destId: ShardId, cursor: Int, count: Int,
               ns: NameServer[TestShard], s: JobScheduler[JsonJob])
extends CopyJob[TestShard](srcId, destId, count, ns, s) {
  def copyPage(src: TestShard, dest: TestShard, count: Int) = {
    val rows = src.getAll(cursor, count)

    dest.putAll(rows)

    if (rows.isEmpty) None
    else Some(new TestCopy(srcId, destId, rows.last._1, count, ns, s))
  }

  def serialize = Map("cursor" -> cursor)
}
