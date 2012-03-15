package com.twitter.gizzard.integration

import scala.collection.JavaConversions._
import com.twitter.gizzard.nameserver.{Host, HostStatus}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.testserver.thrift.TestResult
import com.twitter.gizzard.{IntegrationSpecification, ConfiguredSpecification}


class ReplicationSpec extends IntegrationSpecification with ConfiguredSpecification {
  "Replication" should {
    val servers = List(1, 2, 3).map(testServer)
    val clients = servers.map(testServerClient)

    val server1 :: server2 :: server3 :: _ = servers
    val client1 :: client2 :: client3 :: _ = clients

    val hostFor1 :: hostFor2 :: hostFor3 :: _ = List(server1, server2, server3).map { s =>
      Host("localhost", s.injectorPort, "c" + s.enum, HostStatus.Normal)
    }

    doBefore {
      resetTestServerDBs(servers: _*)
      setupServers(servers: _*)

      List(server1, server2).foreach(_.remoteClusterManager.addRemoteHost(hostFor3))
      List(server1, server3).foreach(_.remoteClusterManager.addRemoteHost(hostFor2))
      List(server2, server3).foreach(_.remoteClusterManager.addRemoteHost(hostFor1))

      servers foreach { _.remoteClusterManager.reload() }
      servers foreach { _.jobAsyncReplicator.reconfigure() }
    }

    doAfter { stopServers(servers: _*) }

    "relay replicated jobs" in {
      startServers(servers: _*)

      client1.put(1, "foo")

      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client2.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client3.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))

      client2.put(2, "bar")

      client1.get(2).toList must eventually(be_==(List(new TestResult(2, "bar", 1))))
      client2.get(2).toList must eventually(be_==(List(new TestResult(2, "bar", 1))))
      client3.get(2).toList must eventually(be_==(List(new TestResult(2, "bar", 1))))

      client3.put(3, "baz")

      client1.get(3).toList must eventually(be_==(List(new TestResult(3, "baz", 1))))
      client2.get(3).toList must eventually(be_==(List(new TestResult(3, "baz", 1))))
      client3.get(3).toList must eventually(be_==(List(new TestResult(3, "baz", 1))))
    }

    "retry replication errors" in {
      startServers(server1)

      client1.put(1, "foo")
      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))

      startServers(server2)
      server1.jobScheduler.retryErrors()

      client2.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))

      startServers(server3)
      server1.jobScheduler.retryErrors()

      client3.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client2.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
    }

    "retry unblocked clusters" in {
      startServers(servers: _*)

      server1.remoteClusterManager.setRemoteClusterStatus("c2", HostStatus.Blocked)
      server1.remoteClusterManager.reload()

      client1.put(1, "foo")
      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client3.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))

      client2.get(1).toList mustEqual List[TestResult]()

      server1.remoteClusterManager.setRemoteClusterStatus("c2", HostStatus.Normal)
      server1.remoteClusterManager.reload()
      server1.jobScheduler.retryErrors()

      client2.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
    }

    "drop blackholed clusters" in {
      startServers(servers: _*)

      server1.remoteClusterManager.setRemoteClusterStatus("c2", HostStatus.Blackholed)
      server1.remoteClusterManager.reload()

      client1.put(1, "foo")
      client1.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))
      client3.get(1).toList must eventually(be_==(List(new TestResult(1, "foo", 1))))

      client2.get(1).toList mustEqual List[TestResult]()

      server1.remoteClusterManager.setRemoteClusterStatus("c2", HostStatus.Normal)
      server1.remoteClusterManager.reload()
      server1.jobScheduler.retryErrors()

      Thread.sleep(200)

      client2.get(1).toList mustEqual List[TestResult]()
    }
  }
}
