package com.twitter.gizzard.integration

import com.twitter.gizzard.thrift.conversions.Sequences._
import testserver.thrift.TestResult

class ReplicationSpec extends IntegrationSpecification with ConfiguredSpecification {
  "Replication" should {
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

    "relay replicated jobs" in {
      startServers(servers: _*)

      client1.put(1, "foo")

      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client2.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client3.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))

      client2.put(2, "bar")

      client1.get(2) must eventually(be_==(List(new TestResult(2, "bar", 1)).toJavaList))
      client2.get(2) must eventually(be_==(List(new TestResult(2, "bar", 1)).toJavaList))
      client3.get(2) must eventually(be_==(List(new TestResult(2, "bar", 1)).toJavaList))

      client3.put(3, "baz")

      client1.get(3) must eventually(be_==(List(new TestResult(3, "baz", 1)).toJavaList))
      client2.get(3) must eventually(be_==(List(new TestResult(3, "baz", 1)).toJavaList))
      client3.get(3) must eventually(be_==(List(new TestResult(3, "baz", 1)).toJavaList))
    }

    "retry replication errors" in {
      startServers(server1)

      client1.put(1, "foo")
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))

      startServers(server2)
      server1.jobScheduler.retryErrors()

      client2.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))

      startServers(server3)
      server1.jobScheduler.retryErrors()

      client3.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client2.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
    }

    "retry unblocked clusters" in {
      startServers(servers: _*)

      server1.nameServer.setRemoteClusterStatus("c2", nameserver.HostStatus.Blocked)
      server1.nameServer.reload()

      client1.put(1, "foo")
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client3.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))

      client2.get(1) mustEqual List[TestResult]().toJavaList

      server1.nameServer.setRemoteClusterStatus("c2", nameserver.HostStatus.Normal)
      server1.nameServer.reload()
      server1.jobScheduler.retryErrors()

      client2.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
    }

    "drop blackholed clusters" in {
      startServers(servers: _*)

      server1.nameServer.setRemoteClusterStatus("c2", nameserver.HostStatus.Blackholed)
      server1.nameServer.reload()

      client1.put(1, "foo")
      client1.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))
      client3.get(1) must eventually(be_==(List(new TestResult(1, "foo", 1)).toJavaList))

      client2.get(1) mustEqual List[TestResult]().toJavaList

      server1.nameServer.setRemoteClusterStatus("c2", nameserver.HostStatus.Normal)
      server1.nameServer.reload()
      server1.jobScheduler.retryErrors()

      Thread.sleep(200)

      client2.get(1) mustEqual List[TestResult]().toJavaList
    }
  }
}
