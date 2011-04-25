package com.twitter.gizzard
package integration

import scala.collection.JavaConversions._
import com.twitter.gizzard.thrift.conversions.Sequences._
import testserver.thrift.TestResult

class RepairSpec extends IntegrationSpecification with ConfiguredSpecification {
  "Repair" should {
    val servers = List(1, 2, 3).map(testServer)
    val clients = servers.map(testServerClient)

    val server1 :: server2 :: server3 :: _ = servers
    val client1 :: client2 :: client3 :: _ = clients

    val hostFor1 :: hostFor2 :: hostFor3 :: _ = List(server1, server2, server3).map { s =>
      nameserver.Host("localhost", s.injectorPort, "c" + s.enum, nameserver.HostStatus.Normal)
    }

    doBefore {
      resetTestServerDBs(servers: _*)
      setupServers(servers: _*)
      List(server1, server2).foreach(_.nameServer.addRemoteHost(hostFor3))
      List(server1, server3).foreach(_.nameServer.addRemoteHost(hostFor2))
      List(server2, server3).foreach(_.nameServer.addRemoteHost(hostFor1))

      servers.foreach(_.nameServer.reload())
    }

    doAfter { stopServers(servers: _*) }

    "differing shards should become the same" in {
      startServers(servers: _*)
      val shard1id :: shard2id :: shard3id :: _ = server1.sqlShardInfos.map(_.id)
      val shard1 :: shard2 :: shard3 :: _ = server1.sqlShardInfos.map(s => server1.nameServer.findShardById(s.id, 0))
      shard1.put(1, "hi")
      shard2.put(2, "hi")
      shard3.put(2, "hi there")
      shard3.put(3, "one")
      shard1.put(4, "this")
      shard1.put(5, "is")
      shard1.put(6, "bulk")
      
      val list = new java.util.ArrayList[com.twitter.gizzard.thrift.ShardId]
      list.add(new com.twitter.gizzard.thrift.ShardId(shard1id.hostname, shard1id.tablePrefix))
      list.add(new com.twitter.gizzard.thrift.ShardId(shard2id.hostname, shard2id.tablePrefix))
      list.add(new com.twitter.gizzard.thrift.ShardId(shard3id.hostname, shard3id.tablePrefix))
      server1.managerServer.repair_shard(list)
      
      def listElemenets(list: Seq[com.twitter.gizzard.testserver.TestResult]) = {
        list.map((e) => (e.id, e.value))
      }
      
      listElemenets(shard1.getAll(0, 100)._1) must eventually(
        verify(s => s sameElements listElemenets(shard2.getAll(0, 100)._1)))
      listElemenets(shard1.getAll(0, 100)._1) must eventually(
        verify(s => s sameElements listElemenets(shard3.getAll(0, 100)._1)))
      listElemenets(shard2.getAll(0, 100)._1) must eventually(
        verify(s => s sameElements listElemenets(shard3.getAll(0, 100)._1)))
    }
  }
}
