package com.twitter.gizzard.nameserver

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.test.NameServerDatabase
import com.twitter.gizzard.ConfiguredSpecification
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class SqlShardSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {

  "SqlShard" should {
    materialize(config)
    val queryEvaluator = evaluator(config)

    val SQL_SHARD = "com.example.SqlShard"

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")

    var nsShard: ShardManagerSource = null
    var remoteClusterShard: RemoteClusterManagerSource = null

    doBefore {
      reset(config)
      nsShard = new SqlShardManagerSource(queryEvaluator)
      nsShard.reload()
      remoteClusterShard = new SqlRemoteClusterManagerSource(queryEvaluator)
      remoteClusterShard.reload()
    }

    "be able to dump nameserver structure" in {
      val shards = (0 to 11).toList.map { i =>
        new ShardInfo("com.twitter.gizzard.fake.NestableShard", "%02d".format(i), "localhost")
      }
      val shardSets = List((0 to 5).toList,(6 to 11).toList).map(_.map(shards))

      shards.foreach { nsShard createShard _ }

      List(0,1).zip(shardSets).foreach { case (tableId, List(a,b,c,d,e,f)) =>
        nsShard.setForwarding(Forwarding(tableId, 0, a.id))
        nsShard.addLink(a.id, b.id, 2)
        nsShard.addLink(a.id, c.id, 2)

        nsShard.setForwarding(Forwarding(tableId, 1, d.id))
        nsShard.addLink(d.id, e.id, 2)
        nsShard.addLink(d.id, f.id, 2)
      }

      val singleStructureList = nsShard.dumpStructure(List(0)).toList
      singleStructureList.length mustEqual 1

      val structureList = nsShard.dumpStructure(List(0,1)).toList
      structureList.length mustEqual 2

      structureList.zip(shardSets).foreach { case (structure, List(a,b,c,d,e,f)) =>
        structure.forwardings.length mustEqual 2
        structure.links.length mustEqual 4
        structure.shards.length mustEqual 6

        structure.shards.sortWith((a,b) => a.tablePrefix.compareTo(b.tablePrefix) < 0) mustEqual List(a,b,c,d,e,f)
      }
    }

    "be able to update nameserver structure" in {
      val shards = (0 to 11).toList.map { i =>
        new ShardInfo("com.twitter.gizzard.fake.NestableShard", "%02d".format(i), "localhost")
      }

      shards.foreach { nsShard createShard _ }
      nsShard.currentState().isEmpty mustEqual true

      nsShard.addLink(shards(0).id, shards(1).id, 2)
      nsShard.currentState().isEmpty mustEqual true

      nsShard.setForwarding(Forwarding(0, 1, shards(0).id))

      val state1 = nsShard.currentState()
      state1.length mustEqual 1
      state1.head.forwardings.toList mustEqual List(Forwarding(0, 1, shards(0).id))
      state1.head.links.length mustEqual 1
      state1.head.shards.toList mustEqual List(shards(0), shards(1))

      nsShard.addLink(shards(0).id, shards(3).id, 2)
      nsShard.setForwarding(Forwarding(0, 2, shards(4).id))

      val state2 = nsShard.currentState()
      state2.length mustEqual 1
      state2.head.forwardings.length mustEqual 2
      state2.head.links.length mustEqual 2
      state2.head.shards.length mustEqual 4

      nsShard.removeLink(shards(0).id, shards(1).id)

      val state3 = nsShard.currentState()
      state3.length mustEqual 1
      state3.head.forwardings.length mustEqual 2
      state3.head.links.length mustEqual 1
      state3.head.shards.length mustEqual 3
    }

    "be idempotent" in {
      "be creatable" in {
        val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
        nsShard.createShard(shardInfo)
        nsShard.createShard(shardInfo)
        nsShard.getShard(shardInfo.id) mustEqual shardInfo
      }

      "be deletable" in {
        val shardInfo = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "table1", "localhost")
        nsShard.createShard(shardInfo)
        nsShard.deleteShard(shardInfo.id)
        nsShard.deleteShard(shardInfo.id)
        nsShard.getShard(shardInfo.id) must throwA[Exception]
      }

      "be linkable and unlinkable" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        val b = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "b", "localhost")
        nsShard.createShard(a)
        nsShard.createShard(b)
        nsShard.addLink(a.id, b.id, 1)
        nsShard.addLink(a.id, b.id, 2)
        nsShard.listUpwardLinks(b.id).head.weight mustEqual 2
        nsShard.removeLink(a.id, b.id)
        nsShard.removeLink(a.id, b.id)
      }

      "be markable busy" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nsShard.createShard(a)
        nsShard.markShardBusy(a.id, Busy.Busy)
        nsShard.markShardBusy(a.id, Busy.Busy)
        nsShard.getShard(a.id).busy mustEqual Busy.Busy
      }

      "sets forwarding" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nsShard.createShard(a)

        nsShard.setForwarding(Forwarding(0, 0, a.id))
        nsShard.setForwarding(Forwarding(0, 0, a.id))
        nsShard.getForwardings.size mustEqual 1
      }

      "removes forwarding" in {
        val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
        nsShard.createShard(a)

        nsShard.setForwarding(Forwarding(0, 0, a.id))

        nsShard.removeForwarding(Forwarding(0, 0, a.id))
        nsShard.removeForwarding(Forwarding(0, 0, a.id))
        nsShard.getForwardings.size mustEqual 0
      }

    }

    "list hostnames" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      nsShard.createShard(a)
      nsShard.listHostnames().head mustEqual "localhost"
    }

    "list tables" in {
      val a = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "a", "localhost")
      val b = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "b", "localhost")
      val c = new ShardInfo("com.twitter.gizzard.fake.NestableShard", "c", "localhost")
      nsShard.createShard(a)
      nsShard.createShard(b)
      nsShard.createShard(c)
      nsShard.setForwarding(Forwarding(0, 0, a.id))
      nsShard.setForwarding(Forwarding(0, 1, b.id))
      nsShard.setForwarding(Forwarding(1, 0, c.id))

      nsShard.listTables must haveTheSameElementsAs(List(0, 1))
    }

    "create" in {
      "a new shard" >> {
        nsShard.createShard(forwardShardInfo)
        nsShard.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          nsShard.createShard(forwardShardInfo)
          nsShard.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
          nsShard.createShard(forwardShardInfo)
          nsShard.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
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
        nsShard.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
        nsShard.getShard(forwardShardInfo.id).className mustEqual forwardShardInfo.className
      }

      "when the shard doesn't exist" >> {
        nsShard.getShard(backwardShardInfo.id) must throwA[NonExistentShard]
      }
    }

    // FIXME: GET SHARD

    "delete" in {
      nsShard.createShard(forwardShardInfo)
      nsShard.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      nsShard.deleteShard(forwardShardInfo.id)
      nsShard.getShard(forwardShardInfo.id) must throwA[NonExistentShard]
    }

    "children" in {
      def shard(i: Int) = ShardId("host", i.toString)
      def linkInfo(up: Int, down: Int, weight: Int) = LinkInfo(shard(up), shard(down), weight)
      def link = linkInfo(1, _: Int, _: Int)

      "add & find" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"))
        nsShard.addLink(shard(1), shard(100), 2)
        nsShard.addLink(shard(1), shard(200), 2)
        nsShard.addLink(shard(1), shard(300), 1)
        nsShard.listDownwardLinks(shard(1)) mustEqual
          List(link(100, 2), link(200, 2), link(300, 1))
      }

      "remove" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"))
        nsShard.addLink(shard(1), shard(100), 2)
        nsShard.addLink(shard(1), shard(200), 2)
        nsShard.addLink(shard(1), shard(300), 1)
        nsShard.removeLink(shard(1), shard(200))
        nsShard.listDownwardLinks(shard(1)) mustEqual List(link(100, 2), link(300, 1))
      }

      "add & remove, retaining order" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "150", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "200", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "300", "host"))
        nsShard.addLink(shard(1), shard(100), 5)
        nsShard.addLink(shard(1), shard(200), 2)
        nsShard.addLink(shard(1), shard(300), 1)
        nsShard.removeLink(shard(1), shard(200))
        nsShard.addLink(shard(1), shard(150), 8)
        nsShard.listDownwardLinks(shard(1)) mustEqual List(link(150, 8), link(100, 5), link(300, 1))
      }

      "link from non-existant shard" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.addLink(shard(23), shard(1), 1) must throwA[Exception]
      }

      "link to non-existant shard" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "23", "host"))
        nsShard.addLink(shard(23), shard(1), 1) must throwA[Exception]
      }

      "remove shard with downlinks" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"))
        nsShard.addLink(shard(1), shard(100), 5)
        nsShard.deleteShard(shard(1)) must throwA[Exception]
      }

      "remove shard with uplinks" >> {
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "1", "host"))
        nsShard.createShard(new ShardInfo("com.twitter.gizzard.fake.NestableShard", "100", "host"))
        nsShard.addLink(shard(1), shard(100), 5)
        nsShard.deleteShard(shard(100)) must throwA[Exception]
      }
    }

    "set shard busy" in {
      nsShard.createShard(forwardShardInfo)
      nsShard.markShardBusy(forwardShardInfo.id, Busy.Busy)
      nsShard.getShard(forwardShardInfo.id).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var forwarding: Forwarding = null

      doBefore {
        nsShard.createShard(forwardShardInfo)
        forwarding = new Forwarding(1, 0L, forwardShardInfo.id)
      }

      "set and get for shard" in {
        nsShard.setForwarding(forwarding)
        nsShard.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replace" in {
        val newShardId = ShardId("new", "shard")
        nsShard.setForwarding(forwarding)
        nsShard.replaceForwarding(forwardShardInfo.id, newShardId)
        nsShard.getForwardingForShard(newShardId).shardId mustEqual newShardId
      }

      "set and get" in {
        nsShard.setForwarding(forwarding)
        nsShard.getForwarding(1, 0L).shardId mustEqual forwardShardInfo.id
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

      doBefore {
        nsShard.createShard(shard1)
        nsShard.createShard(shard2)
        nsShard.createShard(shard3)
        nsShard.addLink(shard1.id, shard2.id, 10)
        nsShard.addLink(shard2.id, shard3.id, 10)
      }

      "shardsForHostname" in {
        nsShard.shardsForHostname("localhost").map { _.id }.sortWith(_.tablePrefix < _.tablePrefix) mustEqual List(shard1.id, shard2.id, shard3.id).sortWith(_.tablePrefix < _.tablePrefix)
      }

      "getBusyShards" in {
        nsShard.getBusyShards() mustEqual List()
        nsShard.markShardBusy(shard1.id, Busy.Busy)
        nsShard.getBusyShards().map { _.id } mustEqual List(shard1.id)
      }

      "getParentShard" in {
        nsShard.listUpwardLinks(shard3.id) mustEqual List(LinkInfo(shard2.id, shard3.id, 10))
        nsShard.listUpwardLinks(shard2.id) mustEqual List(LinkInfo(shard1.id, shard2.id, 10))
        nsShard.listUpwardLinks(shard1.id) mustEqual List()
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
        queryEvaluator.selectOne(sql)(r => true).getOrElse(false) mustEqual true

        remoteClusterShard.addRemoteHost(h)
        remoteClusterShard.listRemoteHosts().length mustEqual 5
      }

      "removeRemoteHost" in {
        remoteClusterShard.getRemoteHost(host1.hostname, host1.port) mustEqual host1

        remoteClusterShard.removeRemoteHost(host1.hostname, host1.port)
        remoteClusterShard.getRemoteHost(host1.hostname, host1.port) must throwA[ShardException]
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
