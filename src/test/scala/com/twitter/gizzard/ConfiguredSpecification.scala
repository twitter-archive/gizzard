package com.twitter.gizzard

import java.io.File
import org.specs.Specification
import net.lag.configgy.Configgy
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.rpcclient.{PooledClient, ThriftConnection}
import com.twitter.util.Eval

import com.twitter.gizzard
import testserver.{Priority, TestServer}
import testserver.config.TestServerConfig



trait ConfiguredSpecification extends Specification {
  noDetailedDiffs()
  val config = Eval[gizzard.config.GizzardServer](new File("config/test.scala"))
  config.logging()
}

trait IntegrationSpecification extends Specification {
  val evaluator = QueryEvaluator("localhost", "", "root", "", Map[String,String]())

  trait TestServerFacts {
    def enum: Int; def nsDatabaseName: String; def databaseName: String
    def basePort: Int; def injectorPort: Int; def managerPort: Int
    def sqlShardInfos: Seq[shards.ShardInfo]
    def replicatingShardInfo: shards.ShardInfo
    def forwarding: nameserver.Forwarding
    def kestrelQueues: Seq[String]
  }

  def testServer(i: Int) = {
    val port = 8000 + (i - 1) * 3
    val name = "testserver" + i
    new TestServer(TestServerConfig(name, port)) with TestServerFacts {
      val enum = i
      val nsDatabaseName = "gizzard_test_"+name+"_ns"
      val databaseName   = "gizzard_test_"+name
      val basePort       = port
      val injectorPort   = port + 1
      val managerPort    = port + 2
      val sqlShardInfos = shards.ShardInfo(shards.ShardId("localhost", "t0_1"),
                                          "TestShard", "int", "int", shards.Busy.Normal) ::
                          shards.ShardInfo(shards.ShardId("localhost", "t0_2"),
                                          "TestShard", "int", "int", shards.Busy.Normal) ::
                          shards.ShardInfo(shards.ShardId("localhost", "t0_3"),
                                          "TestShard", "int", "int", shards.Busy.Normal) :: Nil

      val replicatingShardInfo = shards.ShardInfo(shards.ShardId("localhost", "replicating_0"),
                                          "com.twitter.gizzard.shards.ReplicatingShard", "", "", shards.Busy.Normal)

      val forwarding = nameserver.Forwarding(0, 0, replicatingShardInfo.id)
      val kestrelQueues = Seq("gizzard_test_"+name+"_high_queue",
                              "gizzard_test_"+name+"_high_queue_errors",
                              "gizzard_test_"+name+"_low_queue",
                              "gizzard_test_"+name+"_low_queue_errors")
    }
  }

  type WithFacts = TestServer with TestServerFacts

  def testServerDBs(servers: WithFacts*) = {
    servers.flatMap(s => List(
      "gizzard_test_testserver" + s.enum + "_ns",
      "gizzard_test_testserver" + s.enum))
  }

  def dropTestServerDBs(s: WithFacts*) = testServerDBs(s: _*).foreach { db =>
    evaluator.execute("drop database if exists " + db)
  }

  def createTestServerDBs(s: WithFacts*) = testServerDBs(s: _*).foreach { db =>
    evaluator.execute("create database if not exists " + db)
  }

  def resetTestServerDBs(s: WithFacts*) {
    dropTestServerDBs(s: _*)
    createTestServerDBs(s: _*)
  }

  def testServerClient(s: WithFacts) = {
    val i = s.enum
    val port = 8000 + (i - 1) * 3
    new PooledClient[testserver.thrift.TestServer.Iface] {
      val name = "testclient" + i
      def createConnection =
        new ThriftConnection[testserver.thrift.TestServer.Client]("localhost", port, true)
    }.proxy
  }

  def setupServers(servers: WithFacts*) {
    servers.foreach { s =>
      createTestServerDBs(s)
      s.nameServer.rebuildSchema()
      s.nameServer.setForwarding(s.forwarding)
      s.nameServer.createShard(s.replicatingShardInfo)
      s.sqlShardInfos.foreach(s.nameServer.createShard(_))
      s.sqlShardInfos.foreach(info => s.nameServer.addLink(s.replicatingShardInfo.id, info.id, 1))
      s.nameServer.reload()
    }
  }

  def startServers(servers: TestServer*) {
    servers.foreach(_.start())
    Thread.sleep(100)
  }

  def stopServers(servers: TestServer*) {
    servers.foreach(_.shutdown(true))
    Thread.sleep(100)
  }
}
