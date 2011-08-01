package com.twitter.gizzard
package thrift

import scala.collection.JavaConversions._
import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.ShardId._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import shards.{Busy, RoutingNode}
import scheduler.{CopyJob, CopyJobFactory, JobScheduler, PrioritizingJobScheduler, JsonJob, AdminJobManager}



object ManagerServiceSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val nameServer           = mock[nameserver.NameServer]
  val shardManager         = mock[nameserver.ShardManager]
  val adminJobManager      = mock[AdminJobManager]
  val remoteClusterManager = mock[nameserver.RemoteClusterManager]
  val copier               = mock[CopyJobFactory[AnyRef]]
  val scheduler            = mock[PrioritizingJobScheduler]
  val subScheduler         = mock[JobScheduler]
  val manager              = new ManagerService(nameServer, shardManager, adminJobManager, remoteClusterManager, scheduler)

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
      manager.create_shard(thriftShardInfo1) must throwA[thrift.GizzardException]
    }

    "deliver messages for runtime exceptions" in {
      var woot = false
      expect {
        one(shardManager).createAndMaterializeShard(shardInfo1) willThrow new RuntimeException("Monkeys!")
      }
      try{
        manager.create_shard(thriftShardInfo1)
      } catch {
        case e: thrift.GizzardException => {
          e.getDescription mustEqual "Monkeys!"
          woot = true
        }
      }
      woot mustEqual true
    }

    "create_shard" in {
      expect {
        one(shardManager).createAndMaterializeShard(shardInfo1)
      }
      manager.create_shard(thriftShardInfo1)
    }

    "get_shard" in {
      expect {
        one(shardManager).getShard(shardInfo1.id) willReturn shardInfo1
      }
      manager.get_shard(thriftShardInfo1.id) mustEqual thriftShardInfo1
    }

    "delete_shard" in {
      expect {
        one(shardManager).deleteShard(shardInfo1.id)
      }
      manager.delete_shard(thriftShardInfo1.id)
    }

    "add_link" in {
      expect {
        one(shardManager).addLink(shardInfo1.id, shardInfo2.id, 2)
      }
      manager.add_link(thriftShardInfo1.id, thriftShardInfo2.id, 2)
    }

    "remove_link" in {
      expect {
        one(shardManager).removeLink(shardInfo1.id, shardInfo2.id)
      }
      manager.remove_link(thriftShardInfo1.id, thriftShardInfo2.id)
    }

    "list_downward_links" in {
      expect {
        one(shardManager).listDownwardLinks(shardInfo1.id) willReturn List(shards.LinkInfo(shardInfo1.id, shardInfo2.id, 1))
      }
      manager.list_downward_links(thriftShardInfo1.id).toList mustEqual
        List(new thrift.LinkInfo(thriftShardInfo1.id, thriftShardInfo2.id, 1))
    }

    "mark_shard_busy" in {
      expect {
        one(shardManager).markShardBusy(shardInfo1.id, Busy.Busy)
      }
      manager.mark_shard_busy(thriftShardInfo1.id, Busy.Busy.id)
    }

    "copy_shard" in {
      val shardId1 = new shards.ShardId("hostname1", "table1")
      val shardId2 = new shards.ShardId("hostname2", "table2")

      expect {
        one(adminJobManager).scheduleCopyJob(shardId1, shardId2)
      }

      manager.copy_shard(shardId1.toThrift, shardId2.toThrift)
    }

    "set_forwarding" in {
      expect {
        one(shardManager).setForwarding(forwarding)
      }
      manager.set_forwarding(thriftForwarding)
    }

    "replace_forwarding" in {
      expect {
        one(shardManager).replaceForwarding(shardInfo1.id, shardInfo2.id)
      }
      manager.replace_forwarding(thriftShardInfo1.id, thriftShardInfo2.id)
    }

    "get_forwarding" in {
      expect {
        one(shardManager).getForwarding(tableId, 0) willReturn forwarding
      }
      manager.get_forwarding(tableId, 0) mustEqual thriftForwarding
    }

    "get_forwarding_for_shard" in {
      expect {
        one(shardManager).getForwardingForShard(shardInfo1.id) willReturn forwarding
      }
      manager.get_forwarding_for_shard(thriftShardInfo1.id) mustEqual thriftForwarding
    }

    "get_forwardings" in {
      expect {
        one(shardManager).getForwardings() willReturn List(forwarding)
      }
      manager.get_forwardings().toList mustEqual List(thriftForwarding)
    }

    "reload_config" in {
      expect {
        one(nameServer).reload()
        one(remoteClusterManager).reload()
      }
      manager.reload_config()
    }

    "reload_updated_forwardings" in {
      expect {
        one(nameServer).reloadUpdatedForwardings()
      }
      manager.reload_updated_forwardings()
    }

    "find_current_forwarding" in {
      expect {
        one(nameServer).findCurrentForwarding(tableId, 23L) willReturn shard
        one(shard).shardInfo willReturn shardInfo1
      }
      manager.find_current_forwarding(tableId, 23L) mustEqual thriftShardInfo1
    }

    "shards_for_hostname" in {
      expect {
        one(shardManager).shardsForHostname(hostname) willReturn List(shardInfo1)
      }
      manager.shards_for_hostname(hostname).toList mustEqual List(thriftShardInfo1)
    }

    "get_busy_shards" in {
      expect {
        one(shardManager).getBusyShards() willReturn List(shardInfo1)
      }
      manager.get_busy_shards().toList mustEqual List(thriftShardInfo1)
    }

    "list_upward_links" in {
      expect {
        one(shardManager).listUpwardLinks(shardInfo1.id) willReturn List(shards.LinkInfo(shardInfo2.id, shardInfo1.id, 1))
      }
      manager.list_upward_links(thriftShardInfo1.id).toList mustEqual List(new thrift.LinkInfo(thriftShardInfo2.id, thriftShardInfo1.id, 1))
    }



    // job management

    "retry_errors" in {
      expect {
        one(scheduler).retryErrors()
      }

      manager.retry_errors()
    }

    "stop_writes" in {
      expect {
        one(scheduler).pause()
      }

      manager.stop_writes()
    }

    "resume_writes" in {
      expect {
        one(scheduler).resume()
      }

      manager.resume_writes()
    }

    "retry_errors_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).retryErrors()
      }

      manager.retry_errors_for(3)
    }

    "stop_writes_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).pause()
      }

      manager.stop_writes_for(3)
    }

    "resume_writes_for" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).resume()
      }

      manager.resume_writes_for(3)
    }

    "is_writing" in {
      expect {
        one(scheduler).apply(3) willReturn subScheduler
        one(subScheduler).isShutdown willReturn false
      }

      manager.is_writing(3) mustEqual true
    }

  }
}
