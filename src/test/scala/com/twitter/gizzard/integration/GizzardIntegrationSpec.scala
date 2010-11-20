package com.twitter.gizzard.integration

import org.specs.Specification
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.rpcclient.{PooledClient, ThriftConnection}
import com.twitter.gizzard.thrift.conversions.Sequences._
import testserver.{Priority, TestServer}
import testserver.config.TestServerConfig
import testserver.thrift.TestResult

class GizzardIntegrationSpec extends Specification {

  val evaluator = QueryEvaluator("localhost", "", "root", "", Map[String,String]())

  trait TestServerFacts {
    def enum: Int; def nsDatabaseName: String; def databaseName: String
    def basePort: Int; def injectorPort: Int; def managerPort: Int
    def sqlShardInfo: shards.ShardInfo; def forwarding: nameserver.Forwarding
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
      val sqlShardInfo = shards.ShardInfo(shards.ShardId("localhost", "t0_0"),
                                          "TestShard", "int", "int", shards.Busy.Normal)
      val forwarding = nameserver.Forwarding(0, 0, sqlShardInfo.id)
      val kestrelQueues = Seq("gizzard_test_"+name+"_high_queue",
                              "gizzard_test_"+name+"_high_queue_errors",
                              "gizzard_test_"+name+"_low_queue",
                              "gizzard_test_"+name+"_low_queue_errors")
    }
  }

  def setupServer(server: TestServer with TestServerFacts) {
    createTestServerDBs(server.enum)
    server.nameServer.rebuildSchema()
    server.nameServer.setForwarding(server.forwarding)
    server.nameServer.createShard(server.sqlShardInfo)
    server.nameServer.reload()
  }

  def testServerDBs(i: Int) = List("gizzard_test_testserver" + i + "_ns", "gizzard_test_testserver" + i)
  def dropTestServerDBs(i: Int) = testServerDBs(i).foreach { db =>
    evaluator.execute("drop database if exists " + db)
  }
  def createTestServerDBs(i: Int) = testServerDBs(i).foreach { db =>
    evaluator.execute("create database if not exists " + db)
  }
  def resetTestServerDBs(i: Int) { dropTestServerDBs(i); createTestServerDBs(i) }

  def testServerClient(i: Int) = {
    val port = 8000 + (i - 1) * 3
    new PooledClient[testserver.thrift.TestServer.Iface] {
      val name = "testclient" + i
      def createConnection =
        new ThriftConnection[testserver.thrift.TestServer.Client]("localhost", port, true)
    }
  }

  "TestServer" should {
    var server1 = testServer(1)
    var server2 = testServer(2)
    val client1 = testServerClient(1).proxy
    val client2 = testServerClient(2).proxy

    doBefore {
      Seq(server1, server2).foreach { s => resetTestServerDBs(s.enum); setupServer(s) }
      server1.nameServer.addRemoteHost(nameserver.Host("localhost", server2.injectorPort, "c2", nameserver.HostStatus.Normal))
      server1.nameServer.reload()
      server1.start()
      server2.start()
      Thread.sleep(100)
    }

    doAfter { server1.shutdown(); server2.shutdown() }

    "relay replicated jobs" in {
      client1.put(1, "foo")
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client2.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
    }
  }
}
