package com.twitter.gizzard
package nameserver

import com.twitter.conversions.time._
import com.twitter.gizzard.shards.{Busy, LinkInfo, ShardId, ShardInfo}
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class MemoryShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "MemoryShard" should {
    val SQL_SHARD = "com.example.SqlShard"

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")
    val forwardShardId = new ShardId("localhost", "forward_table")
    val backwardShardId = new ShardId("localhost", "backward_table")
    val shardId1 = new ShardId("localhost", "shard1")
    val shardId2 = new ShardId("localhost", "shard2")
    val shardId3 = new ShardId("localhost", "shard3")
    val shardId4 = new ShardId("localhost", "shard4")
    val shardId5 = new ShardId("localhost", "shard5")

    var nsShard: ShardManagerSource = null
    var remoteClusterShard: RemoteClusterManagerSource = null

    doBefore {
      nsShard            = new MemoryShardManagerSource()
      remoteClusterShard = new MemoryRemoteClusterManagerSource()
    }

    "create" in {
      "a new shard" >> {
        nsShard.createShard(forwardShardInfo)
        nsShard.getShard(forwardShardId) mustEqual forwardShardInfo
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          val shardId = nsShard.createShard(forwardShardInfo)
          nsShard.getShard(forwardShardId) mustEqual forwardShardInfo
          nsShard.createShard(forwardShardInfo)
          nsShard.getShard(forwardShardId) mustEqual forwardShardInfo
        }

        "when the shard contradicts existing data" >> {
          nsShard.createShard(forwardShardInfo)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nsShard.createShard(otherShard) must throwA[InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        nsShard.createShard(forwardShardInfo)
        nsShard.getShard(forwardShardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nsShard.getShard(backwardShardId) must throwA[NonExistentShard]
      }
    }

    "delete" in {
      nsShard.createShard(forwardShardInfo)
      nsShard.getShard(forwardShardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      nsShard.deleteShard(forwardShardId)
      nsShard.getShard(forwardShardId) must throwA[NonExistentShard]
    }

    "links" in {
      "add & find" >> {
        nsShard.addLink(shardId1, shardId2, 3)
        nsShard.addLink(shardId1, shardId3, 2)
        nsShard.addLink(shardId1, shardId4, 1)
        nsShard.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId2, 3), LinkInfo(shardId1, shardId3, 2),
               LinkInfo(shardId1, shardId4, 1))
      }

      "remove" >> {
        nsShard.addLink(shardId1, shardId2, 2)
        nsShard.addLink(shardId1, shardId3, 2)
        nsShard.addLink(shardId1, shardId4, 1)
        nsShard.removeLink(shardId1, shardId3)
        nsShard.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId2, 2), LinkInfo(shardId1, shardId4, 1))
      }

      "add & remove, retaining order" >> {
        nsShard.addLink(shardId1, shardId2, 5)
        nsShard.addLink(shardId1, shardId3, 2)
        nsShard.addLink(shardId1, shardId4, 1)
        nsShard.removeLink(shardId1, shardId3)
        nsShard.addLink(shardId1, shardId5, 8)
        nsShard.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId5, 8), LinkInfo(shardId1, shardId2, 5),
               LinkInfo(shardId1, shardId4, 1))
      }
    }

    "set shard busy" in {
      nsShard.createShard(forwardShardInfo)
      nsShard.markShardBusy(forwardShardId, Busy.Busy)
      nsShard.getShard(forwardShardId).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var forwarding: Forwarding = null

      doBefore {
        nsShard.createShard(forwardShardInfo)
        forwarding = new Forwarding(1, 0L, forwardShardId)
      }

      "set and get for shard" in {
        nsShard.setForwarding(forwarding)
        nsShard.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replace" in {
        nsShard.setForwarding(forwarding)
        nsShard.replaceForwarding(forwarding.shardId, shardId2)
        nsShard.getForwardingForShard(shardId2).shardId mustEqual shardId2
      }

      "set and get" in {
        nsShard.setForwarding(forwarding)
        nsShard.getForwarding(1, 0L).shardId mustEqual forwardShardId
      }

      "get all" in {
        nsShard.setForwarding(forwarding)
        nsShard.getForwardings() mustEqual List(forwarding)
      }
    }

    "advanced shard navigation" in {
      val shard1 = new ShardInfo(SQL_SHARD, "forward_1", "localhost")
      val shard2 = new ShardInfo(SQL_SHARD, "forward_1_also", "localhost")
      val shard3 = new ShardInfo(SQL_SHARD, "forward_1_too", "localhost")
      val shard4 = new ShardInfo(SQL_SHARD, "forward_2", "localhost")

      doBefore {
        nsShard.createShard(shard1)
        nsShard.createShard(shard2)
        nsShard.createShard(shard3)
        nsShard.createShard(shard4)
        nsShard.addLink(shard1.id, shard2.id, 10)
        nsShard.addLink(shard2.id, shard3.id, 10)
        nsShard.setForwarding(Forwarding(0, 0, shard1.id))
        nsShard.setForwarding(Forwarding(0, 1, shard2.id))
        nsShard.setForwarding(Forwarding(1, 0, shard4.id))
      }

      "shardsForHostname" in {
        nsShard.shardsForHostname("localhost").map { _.id }.toList mustEqual List(shard1.id, shard2.id, shard3.id, shard4.id)
      }

      "getBusyShards" in {
        nsShard.getBusyShards() mustEqual List()
        nsShard.markShardBusy(shard1.id, Busy.Busy)
        nsShard.getBusyShards().map { _.id } mustEqual List(shard1.id)
      }

      "listUpwardLinks" in {
        nsShard.listUpwardLinks(shard3.id).map { _.upId }.toList mustEqual List(shard2.id)
        nsShard.listUpwardLinks(shard2.id).map { _.upId }.toList mustEqual List(shard1.id)
        nsShard.listUpwardLinks(shard1.id).map { _.upId }.toList mustEqual List[ShardId]()
      }

      "list tables" in {
        nsShard.listTables must haveTheSameElementsAs(List(0, 1))
      }
    }

    "remote host config management" in {
      val host1 = new Host("remoteapp1", 7777, "c1", HostStatus.Normal)
      val host2 = new Host("remoteapp2", 7777, "c1", HostStatus.Normal)
      val host3 = new Host("remoteapp3", 7777, "c2", HostStatus.Normal)
      val host4 = new Host("remoteapp4", 7777, "c2", HostStatus.Normal)

      doBefore { List(host1, host2, host3, host4).foreach(remoteClusterShard.addRemoteHost) }

      "addRemoteHost" in {
        val h   = new Host("new_host", 7777, "c3", HostStatus.Normal)
        val sql = "SELECT * FROM hosts WHERE hostname = 'new_host' AND port = 7777"

        remoteClusterShard.addRemoteHost(h)
        remoteClusterShard.getRemoteHost(h.hostname, h.port) mustEqual h

        remoteClusterShard.addRemoteHost(h)
        remoteClusterShard.listRemoteHosts().length mustEqual 5
      }

      "removeRemoteHost" in {
        remoteClusterShard.getRemoteHost(host1.hostname, host1.port) mustEqual host1

        remoteClusterShard.removeRemoteHost(host1.hostname, host1.port)
        remoteClusterShard.getRemoteHost(host1.hostname, host1.port) must throwA[shards.ShardException]
      }

      def reloadedHost(h: Host) = remoteClusterShard.getRemoteHost(h.hostname, h.port)

      "setRemoteHostStatus" in {
        remoteClusterShard.setRemoteHostStatus(host1.hostname, host1.port, HostStatus.Blocked)

        reloadedHost(host1).status mustEqual HostStatus.Blocked
        (Set() ++ List(host2, host3, host4).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Normal)
      }

      "setRemoteClusterStatus" in {
        remoteClusterShard.setRemoteClusterStatus("c2", HostStatus.Blackholed)
        (Set() ++ List(host3, host4).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Blackholed)
        (Set() ++ List(host1, host2).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Normal)
      }

      "getRemoteHost" in {
        remoteClusterShard.getRemoteHost(host1.hostname, host1.port) mustEqual host1
      }

      "listRemoteClusters" in {
        remoteClusterShard.listRemoteClusters mustEqual List("c1", "c2")
      }

      "listRemoteHosts" in {
        remoteClusterShard.listRemoteHosts mustEqual List(host1, host2, host3, host4)
      }

      "listRemoteHostsInCluster" in {
        remoteClusterShard.listRemoteHostsInCluster("c1") mustEqual List(host1, host2)
      }
    }
  }
}
