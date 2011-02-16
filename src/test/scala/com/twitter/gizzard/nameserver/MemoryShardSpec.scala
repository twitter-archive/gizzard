package com.twitter.gizzard
package nameserver

import com.twitter.util.TimeConversions._
import com.twitter.gizzard.shards.{Busy, LinkInfo, ShardId, ShardInfo}
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class MemoryShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "MemoryShard" should {
    val SQL_SHARD = "com.example.SqlShard"

    var nameServer: MemoryShard = null
    var shardRepository: ShardRepository[Shard] = null

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")
    val forwardShardId = new ShardId("localhost", "forward_table")
    val backwardShardId = new ShardId("localhost", "backward_table")
    val shardId1 = new ShardId("localhost", "shard1")
    val shardId2 = new ShardId("localhost", "shard2")
    val shardId3 = new ShardId("localhost", "shard3")
    val shardId4 = new ShardId("localhost", "shard4")
    val shardId5 = new ShardId("localhost", "shard5")

    doBefore {
      nameServer = new MemoryShard()
      shardRepository = mock[ShardRepository[Shard]]
    }

    "create" in {
      "a new shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        nameServer.createShard(forwardShardInfo, shardRepository)
        nameServer.getShard(forwardShardId) mustEqual forwardShardInfo
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
            exactly(2).of(shardRepository).create(forwardShardInfo)
          }

          val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.getShard(forwardShardId) mustEqual forwardShardInfo
          nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.getShard(forwardShardId) mustEqual forwardShardInfo
        }

        "when the shard contradicts existing data" >> {
          expect {
            one(shardRepository).create(forwardShardInfo)
          }

          nameServer.createShard(forwardShardInfo, shardRepository)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nameServer.createShard(otherShard, shardRepository) must throwA[InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        nameServer.createShard(forwardShardInfo, shardRepository)
        nameServer.getShard(forwardShardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nameServer.getShard(backwardShardId) must throwA[NonExistentShard]
      }
    }

    "delete" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.getShard(forwardShardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      nameServer.deleteShard(forwardShardId)
      nameServer.getShard(forwardShardId) must throwA[NonExistentShard]
    }

    "links" in {
      "add & find" >> {
        nameServer.addLink(shardId1, shardId2, 3)
        nameServer.addLink(shardId1, shardId3, 2)
        nameServer.addLink(shardId1, shardId4, 1)
        nameServer.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId2, 3), LinkInfo(shardId1, shardId3, 2),
               LinkInfo(shardId1, shardId4, 1))
      }

      "remove" >> {
        nameServer.addLink(shardId1, shardId2, 2)
        nameServer.addLink(shardId1, shardId3, 2)
        nameServer.addLink(shardId1, shardId4, 1)
        nameServer.removeLink(shardId1, shardId3)
        nameServer.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId2, 2), LinkInfo(shardId1, shardId4, 1))
      }

      "add & remove, retaining order" >> {
        nameServer.addLink(shardId1, shardId2, 5)
        nameServer.addLink(shardId1, shardId3, 2)
        nameServer.addLink(shardId1, shardId4, 1)
        nameServer.removeLink(shardId1, shardId3)
        nameServer.addLink(shardId1, shardId5, 8)
        nameServer.listDownwardLinks(shardId1) mustEqual
          List(LinkInfo(shardId1, shardId5, 8), LinkInfo(shardId1, shardId2, 5),
               LinkInfo(shardId1, shardId4, 1))
      }
    }

    "set shard busy" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.markShardBusy(forwardShardId, Busy.Busy)
      nameServer.getShard(forwardShardId).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var forwarding: Forwarding = null

      doBefore {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        nameServer.createShard(forwardShardInfo, shardRepository)
        forwarding = new Forwarding(1, 0L, forwardShardId)
      }

      "set and get for shard" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replace" in {
        nameServer.setForwarding(forwarding)
        nameServer.replaceForwarding(forwarding.shardId, shardId2)
        nameServer.getForwardingForShard(shardId2).shardId mustEqual shardId2
      }

      "set and get" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwarding(1, 0L).shardId mustEqual forwardShardId
      }

      "get all" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardings() mustEqual List(forwarding)
      }
    }

    "advanced shard navigation" in {
      val shard1 = new ShardInfo(SQL_SHARD, "forward_1", "localhost")
      val shard2 = new ShardInfo(SQL_SHARD, "forward_1_also", "localhost")
      val shard3 = new ShardInfo(SQL_SHARD, "forward_1_too", "localhost")
      val shard4 = new ShardInfo(SQL_SHARD, "forward_2", "localhost")

      doBefore {
        expect {
          one(shardRepository).create(shard1)
          one(shardRepository).create(shard2)
          one(shardRepository).create(shard3)
          one(shardRepository).create(shard4)
        }

        nameServer.createShard(shard1, shardRepository)
        nameServer.createShard(shard2, shardRepository)
        nameServer.createShard(shard3, shardRepository)
        nameServer.createShard(shard4, shardRepository)
        nameServer.addLink(shard1.id, shard2.id, 10)
        nameServer.addLink(shard2.id, shard3.id, 10)
        nameServer.setForwarding(Forwarding(0, 0, shard1.id))
        nameServer.setForwarding(Forwarding(0, 1, shard2.id))
        nameServer.setForwarding(Forwarding(1, 0, shard4.id))
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost").map { _.id }.toList mustEqual List(shard1.id, shard2.id, shard3.id, shard4.id)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(shard1.id, Busy.Busy)
        nameServer.getBusyShards().map { _.id } mustEqual List(shard1.id)
      }

      "listUpwardLinks" in {
        nameServer.listUpwardLinks(shard3.id).map { _.upId }.toList mustEqual List(shard2.id)
        nameServer.listUpwardLinks(shard2.id).map { _.upId }.toList mustEqual List(shard1.id)
        nameServer.listUpwardLinks(shard1.id).map { _.upId }.toList mustEqual List[ShardId]()
      }

      "list tables" in {
        nameServer.listTables must haveTheSameElementsAs(List(0, 1))
      }
    }

    "remote host config management" in {
      val host1 = new Host("remoteapp1", 7777, "c1", HostStatus.Normal)
      val host2 = new Host("remoteapp2", 7777, "c1", HostStatus.Normal)
      val host3 = new Host("remoteapp3", 7777, "c2", HostStatus.Normal)
      val host4 = new Host("remoteapp4", 7777, "c2", HostStatus.Normal)

      doBefore { List(host1, host2, host3, host4).foreach(nameServer.addRemoteHost) }

      "addRemoteHost" in {
        val h   = new Host("new_host", 7777, "c3", HostStatus.Normal)
        val sql = "SELECT * FROM hosts WHERE hostname = 'new_host' AND port = 7777"

        nameServer.addRemoteHost(h)
        nameServer.getRemoteHost(h.hostname, h.port) mustEqual h

        nameServer.addRemoteHost(h)
        nameServer.listRemoteHosts().length mustEqual 5
      }

      "removeRemoteHost" in {
        nameServer.getRemoteHost(host1.hostname, host1.port) mustEqual host1

        nameServer.removeRemoteHost(host1.hostname, host1.port)
        nameServer.getRemoteHost(host1.hostname, host1.port) must throwA[shards.ShardException]
      }

      def reloadedHost(h: Host) = nameServer.getRemoteHost(h.hostname, h.port)

      "setRemoteHostStatus" in {
        nameServer.setRemoteHostStatus(host1.hostname, host1.port, HostStatus.Blocked)

        reloadedHost(host1).status mustEqual HostStatus.Blocked
        (Set() ++ List(host2, host3, host4).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Normal)
      }

      "setRemoteClusterStatus" in {
        nameServer.setRemoteClusterStatus("c2", HostStatus.Blackholed)
        (Set() ++ List(host3, host4).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Blackholed)
        (Set() ++ List(host1, host2).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Normal)
      }

      "getRemoteHost" in {
        nameServer.getRemoteHost(host1.hostname, host1.port) mustEqual host1
      }

      "listRemoteClusters" in {
        nameServer.listRemoteClusters mustEqual List("c1", "c2")
      }

      "listRemoteHosts" in {
        nameServer.listRemoteHosts mustEqual List(host1, host2, host3, host4)
      }

      "listRemoteHostsInCluster" in {
        nameServer.listRemoteHostsInCluster("c1") mustEqual List(host1, host2)
      }
    }
  }
}
