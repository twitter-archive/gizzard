package com.twitter.gizzard
package thrift

import com.twitter.util.Future
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.shards.{Busy, RoutingNode}
import com.twitter.gizzard.scheduler._

object ManagerServiceSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val nameServer           = mock[nameserver.NameServer]
  val shardManager         = mock[nameserver.ShardManager]
  val adminJobManager      = mock[AdminJobManager]
  val remoteClusterManager = mock[nameserver.RemoteClusterManager]
  val copier               = mock[CopyJobFactory[AnyRef]]
  val scheduler            = mock[PrioritizingJobScheduler]
  val subScheduler         = mock[JobScheduler]
  val manager              = new ManagerService("manager", 1, nameServer, shardManager, adminJobManager, remoteClusterManager, scheduler)

  val shard = mock[RoutingNode[Nothing]]
  val thriftShardInfo1 = new thrift.ShardInfo(new thrift.ShardId("hostname", "table_prefix"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id)
  val shardInfo1 = new shards.ShardInfo(new shards.ShardId("hostname", "table_prefix"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
  val thriftShardInfo2 = new thrift.ShardInfo(new thrift.ShardId("other_table_prefix", "hostname"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id)
  val shardInfo2 = new shards.ShardInfo(new shards.ShardId("other_table_prefix", "hostname"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
  val hostname = "host1"
  val classname = "com.example.Classname"
  val tableId = 4
  val forwarding = new nameserver.Forwarding(tableId, 0, shardInfo1.id)
  val thriftForwarding = new thrift.Forwarding(tableId, 0, thriftShardInfo1.id)

  "ManagerService" should {
    "explode" in {
      expect {
        one(shardManager).createAndMaterializeShard(shardInfo1) willThrow new shards.ShardException("blarg!")
      }
      manager.createShard(thriftShardInfo1)() must throwA[thrift.GizzardException]
    }

    "deliver messages for runtime exceptions" in {
      var woot = false
      expect {
        one(shardManager).createAndMaterializeShard(shardInfo1) willThrow new RuntimeException("Monkeys!")
      }
      try{
        manager.createShard(thriftShardInfo1)()
      } catch {
        case e: thrift.GizzardException => {
          e.description mustEqual "Monkeys!"
          woot = true
        }
      }
      woot mustEqual true
    }

    "createShard" in {
      expect {
        one(shardManager).createAndMaterializeShard(shardInfo1)
      }
      manager.createShard(thriftShardInfo1)()
    }

    "getShard" in {
      expect {
        one(shardManager).getShard(shardInfo1.id) willReturn shardInfo1
      }
      manager.getShard(thriftShardInfo1.id)() mustEqual thriftShardInfo1
    }

    "deleteShard" in {
      expect {
        one(shardManager).deleteShard(shardInfo1.id)
      }
      manager.deleteShard(thriftShardInfo1.id)()
    }

    "addLink" in {
      expect {
        one(shardManager).addLink(shardInfo1.id, shardInfo2.id, 2)
      }
      manager.addLink(thriftShardInfo1.id, thriftShardInfo2.id, 2)()
    }

    "removeLink" in {
      expect {
        one(shardManager).removeLink(shardInfo1.id, shardInfo2.id)
      }
      manager.removeLink(thriftShardInfo1.id, thriftShardInfo2.id)()
    }

    "listDownwardLinks" in {
      expect {
        one(shardManager).listDownwardLinks(shardInfo1.id) willReturn List(shards.LinkInfo(shardInfo1.id, shardInfo2.id, 1))
      }
      manager.listDownwardLinks(thriftShardInfo1.id)() mustEqual Seq(new thrift.LinkInfo(thriftShardInfo1.id, thriftShardInfo2.id, 1))
    }

    "markShardBusy" in {
      expect {
        one(shardManager).markShardBusy(shardInfo1.id, Busy.Busy)
      }
      manager.markShardBusy(thriftShardInfo1.id, Busy.Busy.id)()
    }

    "copyShard" in {
      val shardId1 = new shards.ShardId("hostname1", "table1")
      val shardId2 = new shards.ShardId("hostname2", "table2")
      val shardIds = Seq(shardId1, shardId2)
      val shardIdsThrift = shardIds map { id=> thrift.ShardId(id.hostname, id.tablePrefix) }

      expect {
        one(adminJobManager).scheduleCopyJob(shardIds)
      }
      manager.copyShard(shardIdsThrift)()
    }

    "setForwarding" in {
      expect {
        one(shardManager).setForwarding(forwarding)
      }
      manager.setForwarding(thriftForwarding)()
    }

    "replaceForwarding" in {
      expect {
        one(shardManager).replaceForwarding(shardInfo1.id, shardInfo2.id)
      }
      manager.replaceForwarding(thriftShardInfo1.id, thriftShardInfo2.id)()
    }

    "getForwarding" in {
      expect {
        one(shardManager).getForwarding(tableId, 0) willReturn forwarding
      }
      manager.getForwarding(tableId, 0)() mustEqual thriftForwarding
    }

    "getForwardingForShard" in {
      expect {
        one(shardManager).getForwardingForShard(shardInfo1.id) willReturn forwarding
      }
      manager.getForwardingForShard(thriftShardInfo1.id)() mustEqual thriftForwarding
    }

    "getForwardings" in {
      expect {
        one(shardManager).getForwardings() willReturn Seq(forwarding)
      }
      manager.getForwardings()() mustEqual Seq(thriftForwarding)
    }

    "reloadConfig" in {
      expect {
        one(nameServer).reload()
        one(remoteClusterManager).reload()
      }
      manager.reloadConfig()
    }

    "reloadUpdatedForwardings" in {
      expect {
        one(nameServer).reloadUpdatedForwardings()
      }
      manager.reloadUpdatedForwardings()
    }

    "findCurrentForwarding" in {
      expect {
        one(nameServer).findCurrentForwarding(tableId, 23L) willReturn shard
        one(shard).shardInfo willReturn shardInfo1
      }
      manager.findCurrentForwarding(tableId, 23L)() mustEqual thriftShardInfo1
    }

    "shardsForHostname" in {
      expect {
        one(shardManager).shardsForHostname(hostname) willReturn Seq(shardInfo1)
      }
      manager.shardsForHostname(hostname)() mustEqual Seq(thriftShardInfo1)
    }

    "getBusyShards" in {
      expect {
        one(shardManager).getBusyShards() willReturn Seq(shardInfo1)
      }
      manager.getBusyShards()() mustEqual Seq(thriftShardInfo1)
    }

    "listUpwardLinks" in {
      expect {
        one(shardManager).listUpwardLinks(shardInfo1.id) willReturn Seq(shards.LinkInfo(shardInfo2.id, shardInfo1.id, 1))
      }
      manager.listUpwardLinks(thriftShardInfo1.id)() mustEqual Seq(new thrift.LinkInfo(thriftShardInfo2.id, thriftShardInfo1.id, 1))
    }



    // job management

    "retryErrors" in {
      expect {
        one(scheduler).retryErrors()
      }

      manager.retryErrors()()
    }

    "stopWrites" in {
      expect {
        one(scheduler).pause()
      }

      manager.stopWrites()()
    }

    "resumeWrites" in {
      expect {
        one(scheduler).resume()
      }

      manager.resumeWrites()()
    }

    "retryErrorsFor" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).retryErrors()
      }

      manager.retryErrorsFor(3)()
    }

    "stopWritesFor" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).pause()
      }

      manager.stopWritesFor(3)()
    }

    "resumeWritesFor" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).resume()
      }

      manager.resumeWritesFor(3)()
    }

    "isWriting" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).isShutdown willReturn false
      }

      manager.isWriting(3)() mustEqual true
    }

  }
}
