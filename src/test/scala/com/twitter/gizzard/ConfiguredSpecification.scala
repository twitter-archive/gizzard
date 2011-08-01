package com.twitter.gizzard

import java.io.File
import org.specs.Specification
import com.twitter.logging.Logger
import com.twitter.querulous.evaluator.QueryEvaluator
import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.thrift.ThriftClientFramedCodec
import com.twitter.util.Eval

import com.twitter.gizzard
import testserver.{Priority, TestServer}
import testserver.config.TestServerConfig


object ConfiguredSpecification {
  val eval = new Eval
}

trait ConfiguredSpecification extends Specification {
  noDetailedDiffs()
  val config =
    try { ConfiguredSpecification.eval[gizzard.config.GizzardServer](new File("config/test.scala")) } catch { case e => e.printStackTrace(); throw e }
  Logger.configure(config.loggers)
}

trait IntegrationSpecification extends Specification {
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
    val client = new testserver.thrift.TestServer.ServiceToClient(ClientBuilder()
        .codec(ThriftClientFramedCodec())
        .hosts(new InetSocketAddress("localhost", port))
        .hostConnectionLimit(1)
        .build(),
        new TBinaryProtocol.Factory())

    new testserver.thrift.TestServer.Iface {
      def put(key: Int, value: String) { client.put(key, value)() }
      def get(key: Int) = client.get(key)()
    }
  }

  def setupServers(servers: WithFacts*) {
    servers.foreach { s =>
      createTestServerDBs(s)
      s.nameServer.reload()
      s.remoteClusterManager.reload()
      s.nameServer.shardManager.setForwarding(s.forwarding)
      s.shardManager.createAndMaterializeShard(s.sqlShardInfo)
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
