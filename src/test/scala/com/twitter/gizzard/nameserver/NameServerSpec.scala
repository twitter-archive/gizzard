package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.configgy.Config


object NameServerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    val nameServerShard = mock[Shard]
    var shardRepository = mock[ShardRepository[shards.Shard]]
    val mappingFunction = (n: Long) => n
    var nameServer: NameServer[shards.Shard] = null

    val shardInfos = (1 until 5).force.map { id =>
      new shards.ShardInfo(shards.ShardId("localhost", id.toString), SQL_SHARD, "a", "b", shards.Busy.Normal)
    }.toList
    val linksList = List(new shards.LinkInfo(shardInfos(2).id, shardInfos(3).id, 1))
    val shardForwardings = List(new Forwarding(1, 1, shardInfos(0).id), new Forwarding(1, 2, shardInfos(1).id),
                                new Forwarding(1, 3, shardInfos(2).id), new Forwarding(2, 1, shardInfos(3).id))

    val nameServerState = NameServerState(shardInfos, linksList, shardForwardings, 1)

    val remoteHosts = List(new Host("host1", 7777, "c1", HostStatus.Normal),
                           new Host("host2", 7777, "c1", HostStatus.Normal),
                           new Host("host3", 7777, "c2", HostStatus.Normal))

    var shard = mock[shards.Shard]

    doBefore {
      expect {
        one(nameServerShard).reload()
        one(nameServerShard).listRemoteHosts() willReturn remoteHosts
        one(nameServerShard).currentState()    willReturn Seq(nameServerState)
      }

      nameServer = new NameServer[gizzard.shards.Shard](
        nameServerShard,
        shardRepository,
        NullJobRelayFactory,
        mappingFunction)
      nameServer.reload()
    }

    "construct from config struct" in {
      val config = new gizzard.config.NameServer {
        mappingFunction = gizzard.config.Fnv1a64
        val replicas    = List(gizzard.config.Memory)
      }

      val ns = config(shardRepository)

      // mapping function should be FNV1A-64:
      ns.mappingFunction(0) mustEqual 632747166973704645L
    }

    "construct from configgy" in {
      val config = new Config()
      config("mapping") = "fnv1a-64"
      config("replicas.ns1.type") = "memory"

      val ns = new gizzard.config.ConfiggyNameServer(config)(shardRepository)

      // mapping function should be FNV1A-64:
      ns.mappingFunction(0) mustEqual 632747166973704645L
    }

    "reload and get shard info" in {
      nameServer.getShardInfo(shardInfos(0).id) mustEqual shardInfos(0)
      nameServer.getShardInfo(shardInfos(1).id) mustEqual shardInfos(1)
      nameServer.getShardInfo(shardInfos(2).id) mustEqual shardInfos(2)
      nameServer.getShardInfo(shardInfos(3).id) mustEqual shardInfos(3)
    }

    "get children" in {
      nameServer.getChildren(shardInfos(2).id).toList mustEqual linksList
    }

    "find current forwarding" in {
      expect {
        one(shardRepository).find(shardInfos(1), 1, List()) willReturn shard
      }

      nameServer.findCurrentForwarding(1, 2) mustEqual shard
    }

    "find shard by id" in {
      expect {
        one(shardRepository).find(shardInfos(3), 1, List()) willReturn shard
        one(shardRepository).find(shardInfos(2), 1, List(shard)) willReturn shard
      }

      nameServer.findShardById(shardInfos(2).id) mustEqual shard
    }

    "find shard by id with a shard not attached to a forwarding" in {
      val floatingShard = shards.ShardInfo(shards.ShardId("localhost", "floating"), SQL_SHARD, "a", "b", shards.Busy.Normal)

      expect {
        one(nameServerShard).getShard(floatingShard.id) willReturn floatingShard
        one(nameServerShard).listDownwardLinks(floatingShard.id) willReturn List[shards.LinkInfo]()
        one(shardRepository).find(floatingShard, 1, List()) willReturn shard
      }

      nameServer.findShardById(floatingShard.id) mustEqual shard
    }

    "create shard" in {
      expect {
//        one(nameServerShard).createShard(shardInfos(0), shardRepository) willThrow new InvalidShard
        one(nameServerShard).createShard(shardInfos(0), shardRepository)
      }
      nameServer.createShard(shardInfos(0)) mustNot throwA[InvalidShard]
    }
  }
}
