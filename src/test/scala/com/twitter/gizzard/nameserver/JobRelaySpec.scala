package com.twitter.gizzard
package nameserver

import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.util.TimeConversions._
import thrift.{JobInjectorService, TThreadServer, JobInjector}


object JobRelaySpec extends ConfiguredSpecification {
  val relay = new JobRelayFactory(2, 1.second)(Map(
    "normal" ->
      Seq(Host("localhost1", 8000, "normal", HostStatus.Normal),
          Host("localhost2", 8000, "normal", HostStatus.Normal)),
    "blackholed" ->
      Seq(Host("localhost3", 8000, "blackholed", HostStatus.Blackholed),
          Host("localhost4", 8000, "blackholed", HostStatus.Blackholed)),
    "blocked" ->
      Seq(Host("localhost5", 8000, "blocked", HostStatus.Blocked),
          Host("localhost6", 8000, "blocked", HostStatus.Blocked)),
    "blackholedAndBlocked" ->
      Seq(Host("localhost11", 8000, "blackholedAndBlocked", HostStatus.Blackholed),
          Host("localhost12", 8000, "blackholedAndBlocked", HostStatus.Blocked)),
    "blockedAndNormal" ->
      Seq(Host("localhost7", 8000, "blockedAndNormal", HostStatus.Blocked),
          Host("localhost8", 8000, "blockedAndNormal", HostStatus.Normal)),
    "blackholedAndNormal" ->
      Seq(Host("localhost9",  8000, "blackholedAndNormal", HostStatus.Blackholed),
          Host("localhost10", 8000, "blackholedAndNormal", HostStatus.Normal)),
    "mixed" ->
      Seq(Host("localhost13", 8000, "mixed", HostStatus.Blackholed),
          Host("localhost14", 8000, "mixed", HostStatus.Blocked),
          Host("localhost15", 8000, "mixed", HostStatus.Normal))
  ))

  "JobRelay" should {
    "return a list of online and blocked clusters" in {
      relay.clusters must haveTheSameElementsAs(List(
        "normal", "blocked", "blackholedAndBlocked", "blockedAndNormal", "blackholedAndNormal", "mixed"
      ))
    }

    "return a normal relay cluster for a set of online hosts" in {
      relay("normal") must notBe(NullJobRelayCluster)
      relay("normal") must notHaveClass[BlockedJobRelayCluster]
    }

    "return a null relay cluster for a nonexistent cluster" in {
      relay("nonexistent") mustEqual NullJobRelayCluster
    }

    "return a null relay cluster for a set of blackholed hosts" in {
      relay("blackholed") mustEqual NullJobRelayCluster
    }

    "return a blocked relay cluster for a set of blocked hosts" in {
      relay("blocked") must haveClass[BlockedJobRelayCluster]
    }

    "return a normal relay cluster for a mixed set of online/blackholed/blocked hosts" in {
      relay("mixed") must notBe(NullJobRelayCluster)
      relay("mixed") must notHaveClass[BlockedJobRelayCluster]

      relay("blockedAndNormal") must notBe(NullJobRelayCluster)
      relay("blockedAndNormal") must notHaveClass[BlockedJobRelayCluster]

      relay("blackholedAndNormal") must notBe(NullJobRelayCluster)
      relay("blackholedAndNormal") must notHaveClass[BlockedJobRelayCluster]
    }

    "return a blocked relay cluster for a mixed set of blocked/blackholed hosts" in {
      relay("blackholedAndBlocked") must haveClass[BlockedJobRelayCluster]
    }
  }
}
