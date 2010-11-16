package com.twitter.gizzard.nameserver

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, ShardId, Busy, LinkInfo}
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.logging.Logger

class SqlShardSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  lazy val poolConfig = config.configMap("db.connection_pool")

  "SqlShard" should {
    materialize(config.configMap("db"))
    val queryEvaluator = evaluator(config.configMap("db"))

    val SQL_SHARD = "com.example.SqlShard"

    var nameServer: SqlShard = null
    var shardRepository: ShardRepository[Shard] = null
    val adapter = { (shard:shards.ReadWriteShard[fake.Shard]) => new fake.ReadWriteShardAdapter(shard) }
    val future = new Future("Future!", 1, 1, 1.second, 1.second)

    val repo = new BasicShardRepository[fake.Shard](adapter, Some(future))
    repo += ("com.twitter.gizzard.fake.NestableShard" -> new fake.NestableShardFactory())

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")

    doBefore {
      nameServer = new SqlShard(queryEvaluator)
      nameServer.rebuildSchema()
      reset(config.configMap("db"))
      shardRepository = mock[ShardRepository[Shard]]
    }

    "be wrappable while replicating" in {
      val nameServerShards = Seq(nameServer)
      val info = new shards.ShardInfo("com.twitter.gizzard.nameserver.Replicatingnameserver.NameServer", "", "")
      val replicationFuture = new Future("ReplicationFuture", 1, 1, new Duration(1), new Duration(1))
      val shard: shards.ReadWriteShard[nameserver.Shard] =
        new shards.ReplicatingShard(info, 0, nameServerShards, new nameserver.LoadBalancer(nameServerShards), Some(replicationFuture))
      val adapted = new nameserver.ReadWriteShardAdapter(shard)
      1 mustEqual 1
    }

    "be idempotent" in {
      "be creatable" in {
        val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
        nameServer.createShard(shardInfo, repo)
        nameServer.createShard(shardInfo, repo)
        nameServer.getShard(shardInfo.id) mustEqual shardInfo
      }

      "be deletable" in {
        val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
        nameServer.createShard(shardInfo, repo)
        nameServer.deleteShard(shardInfo.id)
        nameServer.deleteShard(shardInfo.id)
        nameServer.getShard(shardInfo.id) must throwA[Exception]
      }

      "be linkable and unlinkable" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        val b = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "b", "localhost")
        nameServer.createShard(a, repo)
        nameServer.createShard(b, repo)
        nameServer.addLink(a.id, b.id, 1)
        nameServer.addLink(a.id, b.id, 2)
        nameServer.listUpwardLinks(b.id).first.weight mustEqual 2
        nameServer.removeLink(a.id, b.id)
        nameServer.removeLink(a.id, b.id)
      }

      "be markable busy" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nameServer.createShard(a, repo)
        nameServer.markShardBusy(a.id, shards.Busy.Busy)
        nameServer.markShardBusy(a.id, shards.Busy.Busy)
        nameServer.getShard(a.id).busy mustEqual shards.Busy.Busy
      }

      "sets forwarding" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nameServer.createShard(a, repo)

        nameServer.setForwarding(Forwarding(0, 0, a.id))
        nameServer.setForwarding(Forwarding(0, 0, a.id))
        nameServer.getForwardings.size mustEqual 1
      }

      "removes forwarding" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nameServer.createShard(a, repo)

        nameServer.setForwarding(Forwarding(0, 0, a.id))

        nameServer.removeForwarding(Forwarding(0, 0, a.id))
        nameServer.removeForwarding(Forwarding(0, 0, a.id))
        nameServer.getForwardings.size mustEqual 0
      }

    }

    "list hostnames" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      nameServer.createShard(a, repo)
      nameServer.listHostnames.first mustEqual "localhost"
    }

    "create" in {
      "a new shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        nameServer.createShard(forwardShardInfo, shardRepository)
        nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
            one(shardRepository).create(forwardShardInfo)
          }

          nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
          nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
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
        nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
        nameServer.getShard(forwardShardInfo.id).className mustEqual forwardShardInfo.className
      }

      "when the shard doesn't exist" >> {
        nameServer.getShard(backwardShardInfo.id) must throwA[NonExistentShard]
      }
    }

    // FIXME: GET SHARD

    "delete" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      nameServer.deleteShard(forwardShardInfo.id)
      nameServer.getShard(forwardShardInfo.id) must throwA[NonExistentShard]
    }

    "children" in {
      def shard(i: Int) = ShardId("host", i.toString)
      def linkInfo(up: Int, down: Int, weight: Int) = LinkInfo(shard(up), shard(down), weight)
      def link = linkInfo(1, _: Int, _: Int)

      "add & find" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"), repo)
        nameServer.addLink(shard(1), shard(100), 2)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.listDownwardLinks(shard(1)) mustEqual
          List(link(100, 2), link(200, 2), link(300, 1))
      }

      "remove" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"), repo)
        nameServer.addLink(shard(1), shard(100), 2)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.removeLink(shard(1), shard(200))
        nameServer.listDownwardLinks(shard(1)) mustEqual List(link(100, 2), link(300, 1))
      }

      "add & remove, retaining order" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "150", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"), repo)
        nameServer.addLink(shard(1), shard(100), 5)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.removeLink(shard(1), shard(200))
        nameServer.addLink(shard(1), shard(150), 8)
        nameServer.listDownwardLinks(shard(1)) mustEqual List(link(150, 8), link(100, 5), link(300, 1))
      }

      "link from non-existant shard" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.addLink(shard(23), shard(1), 1) must throwA[Exception]
      }

      "link to non-existant shard" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "23", "host"), repo)
        nameServer.addLink(shard(23), shard(1), 1) must throwA[Exception]
      }

      "remove shard with downlinks" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"), repo)
        nameServer.addLink(shard(1), shard(100), 5)
        nameServer.deleteShard(shard(1)) must throwA[Exception]
      }

      "remove shard with uplinks" >> {
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"), repo)
        nameServer.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"), repo)
        nameServer.addLink(shard(1), shard(100), 5)
        nameServer.deleteShard(shard(100)) must throwA[Exception]
      }
    }

    "set shard busy" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.markShardBusy(forwardShardInfo.id, Busy.Busy)
      nameServer.getShard(forwardShardInfo.id).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var forwarding: Forwarding = null

      doBefore {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        nameServer.createShard(forwardShardInfo, shardRepository)
        forwarding = new Forwarding(1, 0L, forwardShardInfo.id)
      }

      "set and get for shard" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replace" in {
        val newShardId = ShardId("new", "shard")
        nameServer.setForwarding(forwarding)
        nameServer.replaceForwarding(forwardShardInfo.id, newShardId)
        nameServer.getForwardingForShard(newShardId).shardId mustEqual newShardId
      }

      "set and get" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwarding(1, 0L).shardId mustEqual forwardShardInfo.id
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

      doBefore {
        expect {
          one(shardRepository).create(shard1)
          one(shardRepository).create(shard2)
          one(shardRepository).create(shard3)
        }

        nameServer.createShard(shard1, shardRepository)
        nameServer.createShard(shard2, shardRepository)
        nameServer.createShard(shard3, shardRepository)
        nameServer.addLink(shard1.id, shard2.id, 10)
        nameServer.addLink(shard2.id, shard3.id, 10)
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost").map { _.id }.sort(_.tablePrefix < _.tablePrefix) mustEqual List(shard1.id, shard2.id, shard3.id).sort(_.tablePrefix < _.tablePrefix)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(shard1.id, Busy.Busy)
        nameServer.getBusyShards().map { _.id } mustEqual List(shard1.id)
      }

      "getParentShard" in {
        nameServer.listUpwardLinks(shard3.id) mustEqual List(LinkInfo(shard2.id, shard3.id, 10))
        nameServer.listUpwardLinks(shard2.id) mustEqual List(LinkInfo(shard1.id, shard2.id, 10))
        nameServer.listUpwardLinks(shard1.id) mustEqual List()
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
        queryEvaluator.selectOne(sql)(r => true).getOrElse(false) mustEqual true

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
        nameServer.setRemoteClusterStatus("c2", HostStatus.Offline)
        (Set() ++ List(host3, host4).map(reloadedHost(_).status)) mustEqual Set(HostStatus.Offline)
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
