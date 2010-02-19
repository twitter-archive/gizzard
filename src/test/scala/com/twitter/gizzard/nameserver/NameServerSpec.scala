package com.twitter.gizzard.nameserver

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.shards.{ShardInfo, Shard, Busy, ChildInfo}
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{CopyMachine, JobScheduler}


object NameServerSpec extends Specification with JMocker with ClassMocker with Database {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"
    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")
    val otherShardInfo = new ShardInfo(SQL_SHARD, "other_table", "localhost")

    var shardRepository: ShardRepository[Shard] = null
    var forwardingManager: ForwardingManager[Shard] = null
    var copyManager: CopyManager[Shard] = null
    var nameServer: NameServer[Shard] = null
    var forwardShard: Shard = null
    var backwardShard: Shard = null

    doBefore {
      try {
        queryEvaluator.execute("DELETE FROM shards")
      } catch {
        case e: SQLException =>
      }
      try {
        queryEvaluator.execute("DELETE FROM shard_children")
      } catch {
        case e: SQLException =>
      }

      shardRepository = mock[ShardRepository[Shard]]
      forwardingManager = mock[ForwardingManager[Shard]]
      copyManager = mock[CopyManager[Shard]]
      nameServer = new NameServer(queryEvaluator, shardRepository, forwardingManager, copyManager)

      expect {
        one(forwardingManager).reloadForwardings(nameServer)
      }
      nameServer.reload()

      forwardShard = mock[Shard]
      backwardShard = mock[Shard]
    }

    "create" in {
      "a new shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
            exactly(2).of(shardRepository).create(forwardShardInfo)
          }

          val shardId = nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
          nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
        }

        "when the shard contradicts existing data" >> {
          expect {
            one(shardRepository).create(forwardShardInfo)
          }

          nameServer.createShard(forwardShardInfo)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nameServer.createShard(otherShard) must throwA[NameServer.InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
        nameServer.getShard(shardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nameServer.findShard(backwardShardInfo) must throwA[NameServer.NonExistentShard]
      }
    }

    "update" in {
      "existing shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        val otherShardInfo = forwardShardInfo.clone
        otherShardInfo.tablePrefix = "other_table"
        otherShardInfo.shardId = shardId
        nameServer.updateShard(otherShardInfo)
        nameServer.getShard(shardId) mustEqual otherShardInfo
      }

      "nonexistent shard" >> {
        nameServer.updateShard(new ShardInfo(SQL_SHARD, "other_table", "localhost", "", "", Busy.Normal, 500)) must throwA[NameServer.NonExistentShard]
      }
    }

    "delete" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      val shardId = nameServer.createShard(forwardShardInfo)
      nameServer.findShard(forwardShardInfo) mustEqual shardId
      nameServer.deleteShard(shardId)
      nameServer.findShard(forwardShardInfo) must throwA[NameServer.NonExistentShard]
    }

    "add & find children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.listShardChildren(1) mustEqual
        List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(300, 1))
    }

    "remove children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.removeChildShard(1, 200)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(300, 1))
    }

    "add & remove children, retaining order" in {
      nameServer.addChildShard(1, 100, 5)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.removeChildShard(1, 200)
      nameServer.addChildShard(1, 150, 8)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(150, 8), ChildInfo(100, 5), ChildInfo(300, 1))
    }

    "replace children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.replaceChildShard(300, 400)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(400, 1))
    }

    "set shard busy" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      val shardId = nameServer.createShard(forwardShardInfo)
      nameServer.markShardBusy(shardId, Busy.Busy)
      nameServer.getShard(shardId).busy mustEqual Busy.Busy
    }

    "copy shard" in {
      val copyJob = mock[CopyMachine[Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newCopyJob(10, 20) willReturn copyJob
        one(copyManager).scheduler willReturn scheduler
        one(copyJob).start(nameServer, scheduler)
      }

      nameServer.copyShard(10, 20)
    }

    "setup migration" in {
      val sourceShardInfo = new ShardInfo("com.example.SqlShard", "forward", "localhost")
      val destinationShardInfo = new ShardInfo("com.example.SqlShard", "forward", "remotehost")
      val writeOnlyShardInfo = new ShardInfo("com.example.WriteOnlyShard",
                                             "forward_migrate_write_only", "localhost")
      val replicatingShardInfo = new ShardInfo("com.example.ReplicatingShard",
                                               "forward_migrate_replicating", "localhost")

      expect {
        one(shardRepository).create(sourceShardInfo)
      }

      val sourceShardId = nameServer.createShard(sourceShardInfo)
      val forwardingSourceId = capturingParam[Int]
      val forwardingDestinationId = capturingParam[Int]

      expect {
        one(shardRepository).create(destinationShardInfo)
        one(shardRepository).create(writeOnlyShardInfo)
        one(shardRepository).create(replicatingShardInfo)
        one(forwardingManager).replaceForwarding(forwardingSourceId.capture(0), forwardingDestinationId.capture(1))
      }

      val migration = nameServer.setupMigration(sourceShardInfo, destinationShardInfo)
      nameServer.getShard(migration.sourceShardId).shardId mustEqual sourceShardId
      nameServer.findShard(destinationShardInfo) mustEqual migration.destinationShardId
      nameServer.findShard(writeOnlyShardInfo) mustEqual migration.writeOnlyShardId
      nameServer.findShard(replicatingShardInfo) mustEqual migration.replicatingShardId

      forwardingSourceId.captured mustEqual sourceShardId
      forwardingDestinationId.captured mustEqual migration.replicatingShardId

      nameServer.listShardChildren(migration.replicatingShardId).map { _.shardId } mustEqual
        List(migration.sourceShardId, migration.writeOnlyShardId)
      nameServer.listShardChildren(migration.writeOnlyShardId).map { _.shardId } mustEqual
        List(migration.destinationShardId)
    }

    "migrate shard" in {
      val migration = new ShardMigration(1, 2, 3, 4)
      val migrateJob = mock[CopyMachine[Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newMigrateJob(migration) willReturn migrateJob
        one(copyManager).scheduler willReturn scheduler
        one(migrateJob).start(nameServer, scheduler)
      }

      nameServer.migrateShard(migration)
    }

    "finish migration" in {
      // tediously set this up.
      val sourceShardInfo = new ShardInfo("com.example.SqlShard", "forward", "localhost")
      val destinationShardInfo = new ShardInfo("com.example.SqlShard", "forward", "remotehost")
      val writeOnlyShardInfo = new ShardInfo("com.example.WriteOnlyShard",
                                             "forward_migrate_write_only", "localhost")
      val replicatingShardInfo = new ShardInfo("com.example.ReplicatingShard",
                                               "forward_migrate_replicating", "localhost")

      expect {
        one(shardRepository).create(sourceShardInfo)
        one(shardRepository).create(destinationShardInfo)
        one(shardRepository).create(writeOnlyShardInfo)
        one(shardRepository).create(replicatingShardInfo)
      }

      val sourceShardId = nameServer.createShard(sourceShardInfo)
      val destinationShardId = nameServer.createShard(destinationShardInfo)
      val writeOnlyShardId = nameServer.createShard(writeOnlyShardInfo)
      val replicatingShardId = nameServer.createShard(replicatingShardInfo)
      nameServer.addChildShard(replicatingShardId, sourceShardId, 10)
      nameServer.addChildShard(replicatingShardId, writeOnlyShardId, 10)
      nameServer.addChildShard(writeOnlyShardId, destinationShardId, 20)

      val migration = new ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)

      expect {
        one(forwardingManager).replaceForwarding(replicatingShardId, destinationShardId)
      }

      nameServer.finishMigration(migration)

      nameServer.getShard(sourceShardId) must throwA[Exception]
      nameServer.getShard(destinationShardId)
      nameServer.getShard(writeOnlyShardId) must throwA[Exception]
      nameServer.getShard(replicatingShardId) must throwA[Exception]
    }

    "forwarding changes" in {
      var shardId: Int = 0
      var forwarding: Forwarding = null

      doBefore {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        shardId = nameServer.createShard(forwardShardInfo)
        forwarding = new Forwarding(List(1, 2), 0L, shardId)
      }

      "setForwarding" in {
        expect {
          one(forwardingManager).setForwarding(forwarding)
        }
        nameServer.setForwarding(forwarding)
      }

      "replaceForwarding" in {
        expect {
          one(forwardingManager).replaceForwarding(1, 2)
        }
        nameServer.replaceForwarding(1, 2)
      }

      "getForwarding" in {
        expect {
          one(forwardingManager).getForwarding(List(1, 2), 0L) willReturn shardId
        }
        nameServer.getForwarding(List(1, 2), 0L).shardId mustEqual shardId
      }

      "getForwardingForShard" in {
        expect {
          one(forwardingManager).getForwardingForShard(9) willReturn forwarding
        }
        nameServer.getForwardingForShard(9) mustEqual forwarding
      }

      "getForwardings" in {
        expect {
          one(forwardingManager).getForwardings() willReturn List(forwarding)
        }
        nameServer.getForwardings() mustEqual List(forwarding)
      }

      "findCurrentForwarding" in {
        expect {
          one(forwardingManager).findCurrentForwarding(List(1, 2), 0L) willReturn forwardShard
        }
        nameServer.findCurrentForwarding(List(1, 2), 0L) mustEqual forwardShard
      }
    }

    "advanced shard navigation" in {
      val shard1 = new ShardInfo(SQL_SHARD, "forward_1", "localhost")
      val shard2 = new ShardInfo(SQL_SHARD, "forward_1_also", "localhost")
      val shard3 = new ShardInfo(SQL_SHARD, "forward_1_too", "localhost")
      var id1 = 0
      var id2 = 0
      var id3 = 0

      doBefore {
        expect {
          one(shardRepository).create(shard1)
          one(shardRepository).create(shard2)
          one(shardRepository).create(shard3)
        }
        id1 = nameServer.createShard(shard1)
        id2 = nameServer.createShard(shard2)
        id3 = nameServer.createShard(shard3)
        nameServer.addChildShard(id1, id2, 10)
        nameServer.addChildShard(id2, id3, 10)
      }

      "shardIdsForHostname" in {
        nameServer.shardIdsForHostname("localhost", SQL_SHARD) mustEqual List(id1, id2, id3)
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost", SQL_SHARD).map { _.shardId } mustEqual List(id1, id2, id3)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(id1, Busy.Busy)
        nameServer.getBusyShards().map { _.shardId } mustEqual List(id1)
      }

      "getParentShard" in {
        nameServer.getParentShard(id3).shardId mustEqual id2
        nameServer.getParentShard(id2).shardId mustEqual id1
        nameServer.getParentShard(id1).shardId mustEqual id1
      }

      "getRootShard" in {
        nameServer.getRootShard(id3).shardId mustEqual id1
      }

      "getChildShardsOfClass" in {
        nameServer.getChildShardsOfClass(id1, SQL_SHARD).map { _.shardId } mustEqual List(id2, id3)
      }
    }
  }
}
